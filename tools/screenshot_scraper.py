#!/usr/bin/env python3

import requests
import click
import tempfile
import shutil

from requests.exceptions import SSLError
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import JavascriptException
# from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import UnexpectedAlertPresentException

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound

from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db):
    return db.dns.find(
        {
            'image': {'$exists': False},
            'image_scan_failed': {'$exists': False},
            'ports.port': {'$in': [80, 443]},
        }
    ).sort([('updated', -1)])


def update_data(db, domain, post):
    try:
        res = db.dns.update_one({'domain': domain}, {'$set': post}, upsert=False)

        if res.modified_count > 0:
            print(u'INFO: updated domain {} image path'.format(domain))
        else:
            update_data_error(db, domain)
    except DuplicateKeyError:
        return


def update_data_error(db, domain):
    db.dns.update_one({'domain': domain},
                      {'$set': {'image_scan_failed': datetime.now()}}, upsert=False)


def request_javasript(url):
    try:
        r = requests.get(url)
    except SSLError:
        return ''

    if r.status_code == 200:
        return r.text.strip('\n')
    else:
        return ''


@click.command()
@click.option('--host', required=True, help='MongoDB host to connect to')
def main(host):
    client = connect(host)
    db = client.ip_data

    try:
        domains = retrieve_domains(db)
    except CursorNotFound:
        return

    for domain in domains:
        url = 'https://{}'.format(domain['domain'])
        print(u'INFO: proceed with domain {}'.format(domain['domain']))

        options = webdriver.ChromeOptions()
        options.binary_location = '/usr/bin/chromium-browser'
        # Harden headless launch for container/CI environments.
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-infobars')
        options.add_argument('--window-size=1200,800')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-gpu')
        options.add_argument('--remote-debugging-port=9222')
        options.add_argument('--disable-software-rasterizer')

        user_data_dir = tempfile.mkdtemp(prefix='chrome-profile-')
        options.add_argument(f'--user-data-dir={user_data_dir}')

        chromedriver_path = shutil.which('chromedriver') or '/snap/bin/chromium'
        if not chromedriver_path or not Path(chromedriver_path).exists():
            print("ERROR: chromedriver binary not found or not executable")
            update_data_error(db, domain['domain'])
            shutil.rmtree(user_data_dir, ignore_errors=True)
            continue

        service = Service(executable_path=chromedriver_path)

        try:
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            print(f"ERROR: Failed to initialize WebDriver for {domain['domain']}: {e}")
            update_data_error(db, domain['domain'])
            shutil.rmtree(user_data_dir, ignore_errors=True)
            continue

        image_name = None
        success = False

        try:
            driver.set_page_load_timeout(15)
            driver.get(url)

            for item in driver.find_elements(By.XPATH, '//script'):
                try:
                    if item.get_attribute('src'):
                        try:
                            driver.execute_script(request_javasript(item.get_attribute('src')))
                        except UnexpectedAlertPresentException:
                            continue
                    else:
                        driver.execute_script(item.text)
                except (JavascriptException, StaleElementReferenceException):
                    continue

            image_name = '{}.png'.format(domain['domain'])
            driver.save_screenshot('screenshots/{}'.format(image_name))
            print(f"INFO: saved screenshot to screenshots/{image_name}")
            success = True
        except TimeoutException:
            print(f"ERROR: Timeout while loading {url}")
            update_data_error(db, domain['domain'])
        except Exception as exc:
            print(f"ERROR: Failed to capture screenshot for {domain['domain']}: {exc}")
            update_data_error(db, domain['domain'])
        finally:
            driver.quit()
            shutil.rmtree(user_data_dir, ignore_errors=True)

        if not success or not image_name:
            continue

        data = {'updated': datetime.now(), 'image': image_name}

        update_data(db, domain['domain'], data)

    client.close()


if __name__ == '__main__':
    main()
