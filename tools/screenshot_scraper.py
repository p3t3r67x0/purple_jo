"""Capture HTTPS screenshots for domains stored in PostgreSQL."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import click
import requests
from requests.exceptions import SSLError
from selenium import webdriver
from selenium.common.exceptions import (
    JavascriptException,
    StaleElementReferenceException,
    TimeoutException,
    UnexpectedAlertPresentException,
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from shared.models.postgres import Domain, PortService
from tools.sqlmodel_helpers import get_engine, resolve_sync_dsn, session_scope

SCREENSHOT_DIR = Path("screenshots")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_columns(engine: Engine) -> None:
    inspector = inspect(engine)
    columns = {column["name"] for column in inspector.get_columns("domains")}

    statements: List[str] = []
    if "image" not in columns:
        statements.append("ALTER TABLE domains ADD COLUMN IF NOT EXISTS image TEXT")
    if "image_scan_failed" not in columns:
        statements.append(
            "ALTER TABLE domains ADD COLUMN IF NOT EXISTS image_scan_failed "
            "TIMESTAMP WITH TIME ZONE"
        )

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


@dataclass
class CandidateDomain:
    id: int
    name: str


def fetch_candidate_domains(session: Session, limit: Optional[int]) -> List[CandidateDomain]:
    statement = (
        select(Domain.id, Domain.name)
        .where(Domain.image.is_(None))
        .where(Domain.image_scan_failed.is_(None))
        .where(
            select(PortService.id)
            .where(PortService.domain_id == Domain.id)
            .where(PortService.port.in_([80, 443]))
            .exists()
        )
        .order_by(Domain.updated_at.desc())
    )

    if limit is not None:
        statement = statement.limit(limit)

    results = session.exec(statement).all()
    return [CandidateDomain(id=row[0], name=row[1]) for row in results]


def mark_success(session: Session, domain_id: int, image_name: str) -> None:
    domain = session.get(Domain, domain_id)
    if not domain:
        return

    now = utcnow()
    domain.image = image_name
    domain.image_scan_failed = None
    domain.updated_at = now
    session.add(domain)


def mark_failure(session: Session, domain_id: int) -> None:
    domain = session.get(Domain, domain_id)
    if not domain:
        return

    now = utcnow()
    domain.image_scan_failed = now
    domain.updated_at = now
    session.add(domain)


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

    checked_paths: List[str] = []
    chromedriver_path = shutil.which("chromedriver")
    if chromedriver_path:
        checked_paths.append(chromedriver_path)
        if not Path(chromedriver_path).exists() or not os.access(chromedriver_path, os.X_OK):
            chromedriver_path = None

    if not chromedriver_path:
        fallback_paths = [
            "/snap/bin/chromium.chromedriver",
            "/usr/lib/chromium-browser/chromedriver",
            "/usr/lib/chromium/chromedriver",
        ]
        for path in fallback_paths:
            checked_paths.append(path)
            if Path(path).exists() and os.access(path, os.X_OK):
                chromedriver_path = path
                break

    if not chromedriver_path:
        inspected = ", ".join(checked_paths) if checked_paths else "(no paths inspected)"
        print(
            "[ERROR] chromedriver binary not found or not executable. "
            f"Checked paths: {inspected}"
        )
        shutil.rmtree(user_data_dir, ignore_errors=True)
        return None

    service = Service(executable_path=chromedriver_path)

    try:
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as exc:
        print(f"[ERROR] Failed to initialise Chrome driver: {exc}")
        shutil.rmtree(user_data_dir, ignore_errors=True)
        return None

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

    dsn = resolve_sync_dsn(postgres_dsn)
    engine = get_engine(dsn)

    ensure_columns(engine)

    with session_scope(engine=engine) as session:
        candidates = fetch_candidate_domains(session, limit)

    if not candidates:
        click.echo("[INFO] No domains require screenshots")
        return

    click.echo(f"[INFO] Processing {len(candidates)} domains")

    driver = initialise_webdriver()
    if not driver:
        click.echo("[ERROR] Unable to initialise WebDriver; marking domains as failed")
        with session_scope(engine=engine) as session:
            for domain in candidates:
                mark_failure(session, domain.id)
                session.commit()
        return

    try:
        with session_scope(engine=engine) as session:
            for domain in candidates:
                click.echo(f"[INFO] Processing {domain.name}")

                if driver is None:
                    driver = initialise_webdriver()
                    if not driver:
                        click.echo("[ERROR] Unable to recover WebDriver session; marking failure")
                        mark_failure(session, domain.id)
                        session.commit()
                        continue

                image_name = capture_screenshot(driver, domain.name)

                if getattr(driver, "session_id", None) is None:
                    cleanup_driver(driver)
                    driver = None

                if driver is not None:
                    try:
                        driver.delete_all_cookies()
                    except Exception as exc:  # pragma: no cover - defensive cleanup
                        print(f"[WARNING] Failed to clear cookies: {exc}")
                        cleanup_driver(driver)
                        driver = None

                if image_name:
                    mark_success(session, domain.id, image_name)
                else:
                    mark_failure(session, domain.id)
                session.commit()
    finally:
        if driver is not None:
            cleanup_driver(driver)

    click.echo("[INFO] Screenshot processing complete")


if __name__ == "__main__":
    main()
