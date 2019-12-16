#!/usr/bin/env python3

import requests

from requests.exceptions import SSLError

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import JavascriptException
# from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import StaleElementReferenceException

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_domains(db):
    return db.dns.find({'image': {'$exists': False},
                        'image_scan_failed': {'$exists': False}
                        }).sort([('updated', -1)])


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
                      {'$set': {'image_scan_failed': datetime.utcnow()}}, upsert=False)


def request_javasript(url):
    try:
        r = requests.get(url)
    except SSLError:
        return ''

    if r.status_code == 200:
        return r.text.strip('\n')
    else:
        return ''


def main():
    client = connect()
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
        options.add_argument('headless')
        options.add_argument('disable-infobars')
        options.add_argument('window-size=1200x800')
        options.add_argument('start-maximized')

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(15)

        try:
            driver.get(url)
        except TimeoutException:
            continue

        for item in driver.find_elements(By.XPATH, '//script'):
            try:
                if item.get_attribute('src'):
                    driver.execute_script(request_javasript(item.get_attribute('src')))
                else:
                    driver.execute_script(item.text)
            except (JavascriptException, StaleElementReferenceException):
                continue

        image_name = '{}.png'.format(domain['domain'])
        driver.save_screenshot('screenshots/{}'.format(image_name))
        data = {'updated': datetime.utcnow(), 'image': image_name}

        update_data(db, domain['domain'], data)


if __name__ == '__main__':
    main()
