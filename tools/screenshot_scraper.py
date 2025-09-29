#!/usr/bin/env python3

"""Capture HTTPS screenshots for domains stored in PostgreSQL."""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import click
import psycopg
from psycopg.rows import dict_row
import requests
from requests.exceptions import SSLError
from selenium import webdriver
from selenium.common.exceptions import (
    JavascriptException,
    TimeoutException,
    UnexpectedAlertPresentException,
    StaleElementReferenceException,
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
SCREENSHOT_DIR = Path("screenshots")


def _parse_env_file(path: Path) -> dict[str, str]:
    env_vars: dict[str, str] = {}
    if not path.exists():
        return env_vars

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_vars[key.strip()] = value.strip().strip('"\'')
    return env_vars


def resolve_dsn(explicit: Optional[str] = None) -> str:
    if explicit:
        dsn = explicit
    elif "POSTGRES_DSN" in os.environ:
        dsn = os.environ["POSTGRES_DSN"]
    else:
        env_vars = _parse_env_file(ENV_PATH)
        dsn = env_vars.get("POSTGRES_DSN")

    if not dsn:
        raise ValueError("POSTGRES_DSN not provided via flag, env var, or .env file")

    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    elif dsn.startswith("postgresql+psycopg://"):
        dsn = "postgresql://" + dsn[len("postgresql+psycopg://") :]
    elif "://" not in dsn:
        dsn = f"postgresql://{dsn}"

    return dsn


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_columns(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE domains ADD COLUMN IF NOT EXISTS image TEXT")
        cur.execute(
            "ALTER TABLE domains ADD COLUMN IF NOT EXISTS image_scan_failed TIMESTAMP WITH TIME ZONE"
        )
    conn.commit()


def fetch_candidate_domains(
    conn: psycopg.Connection, limit: Optional[int]
) -> List[Dict[str, str]]:
    query = """
        SELECT d.id, d.name
        FROM domains d
        WHERE d.image IS NULL
          AND d.image_scan_failed IS NULL
          AND EXISTS (
                SELECT 1 FROM port_services ps
                WHERE ps.domain_id = d.id AND ps.port IN (80, 443)
          )
        ORDER BY d.updated_at DESC
    """
    params: List[object] = []
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def mark_success(conn: psycopg.Connection, domain_id: int, image_name: str) -> None:
    now = utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE domains
            SET image = %s,
                image_scan_failed = NULL,
                updated_at = %s
            WHERE id = %s
            """,
            (image_name, now, domain_id),
        )
    conn.commit()


def mark_failure(conn: psycopg.Connection, domain_id: int) -> None:
    now = utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE domains
            SET image_scan_failed = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (now, now, domain_id),
        )
    conn.commit()


def request_javascript(url: str) -> str:
    try:
        response = requests.get(url, timeout=10)
    except SSLError:
        return ""
    except Exception as exc:
        print(f"[WARNING] Failed to fetch script {url}: {exc}")
        return ""

    if response.status_code == 200:
        return response.text.strip("\n")
    return ""


def initialise_webdriver() -> Optional[webdriver.Chrome]:
    options = webdriver.ChromeOptions()
    options.binary_location = "/usr/bin/chromium-browser"
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-infobars")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-software-rasterizer")

    user_data_dir = tempfile.mkdtemp(prefix="chrome-profile-")
    options.add_argument(f"--user-data-dir={user_data_dir}")

    chromedriver_path = shutil.which("chromedriver") or "/snap/bin/chromium"
    if not chromedriver_path or not Path(chromedriver_path).exists():
        print("[ERROR] chromedriver binary not found or not executable")
        shutil.rmtree(user_data_dir, ignore_errors=True)
        return None

    service = Service(executable_path=chromedriver_path)

    try:
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as exc:
        print(f"[ERROR] Failed to initialize WebDriver: {exc}")
        shutil.rmtree(user_data_dir, ignore_errors=True)
        return None

    # Attach the temp directory for later cleanup
    driver._screenshot_user_data_dir = user_data_dir  # type: ignore[attr-defined]
    return driver


def cleanup_driver(driver: webdriver.Chrome) -> None:
    user_data_dir = getattr(driver, "_screenshot_user_data_dir", None)
    try:
        driver.quit()
    finally:
        if user_data_dir:
            shutil.rmtree(user_data_dir, ignore_errors=True)


def capture_screenshot(driver: webdriver.Chrome, domain: str) -> Optional[str]:
    url = f"https://{domain}"
    try:
        driver.set_page_load_timeout(15)
        driver.get(url)

        for item in driver.find_elements(By.XPATH, "//script"):
            try:
                src = item.get_attribute("src")
                if src:
                    script = request_javascript(src)
                    if script:
                        driver.execute_script(script)
                else:
                    driver.execute_script(item.text)
            except UnexpectedAlertPresentException:
                continue
            except (JavascriptException, StaleElementReferenceException):
                continue

        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        image_name = f"{domain}.png"
        driver.save_screenshot(str(SCREENSHOT_DIR / image_name))
        print(f"INFO: saved screenshot to {SCREENSHOT_DIR / image_name}")
        return image_name
    except TimeoutException:
        print(f"[ERROR] Timeout while loading {url}")
    except Exception as exc:
        print(f"[ERROR] Failed to capture screenshot for {domain}: {exc}")
    return None


@click.command()
@click.option(
    "--postgres-dsn",
    default=None,
    help="PostgreSQL DSN (overrides POSTGRES_DSN env/.env)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Optional limit on the number of domains to process",
)
def main(postgres_dsn: Optional[str], limit: Optional[int]) -> None:
    """Capture HTTPS screenshots for domains missing image data."""

    dsn = resolve_dsn(postgres_dsn)

    with psycopg.connect(dsn) as conn:
        ensure_columns(conn)
        candidates = fetch_candidate_domains(conn, limit)

    if not candidates:
        click.echo("[INFO] No domains require screenshots")
        return

    click.echo(f"[INFO] Processing {len(candidates)} domains")

    for domain in candidates:
        domain_id = int(domain["id"])
        domain_name = domain["name"]
        click.echo(f"[INFO] Processing {domain_name}")

        driver = initialise_webdriver()
        if not driver:
            with psycopg.connect(dsn) as conn:
                mark_failure(conn, domain_id)
            continue

        image_name = capture_screenshot(driver, domain_name)
        cleanup_driver(driver)

        with psycopg.connect(dsn) as conn:
            if image_name:
                mark_success(conn, domain_id, image_name)
            else:
                mark_failure(conn, domain_id)

    click.echo("[INFO] Screenshot processing complete")


if __name__ == "__main__":
    main()
