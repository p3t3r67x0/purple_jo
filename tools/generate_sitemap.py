#!/usr/bin/env python3

import argparse

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import JavascriptException
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

from lxml import etree


def load_sitemap(filename):
    with open(filename, 'r') as f:
        return f.read()


def retrieve_sitemap(input):
    xml = load_sitemap(input)
    root = etree.XML(xml.encode())
    sitemap = []

    for elm in root:
        children = elm.getchildren()
        loc = {'loc': children[0].text}
        sitemap.append([loc])

    return sitemap


def create_sitemap(urls, output):
    urlset = etree.Element('urlset')
    urlset.attrib['xmlns'] = 'http://www.sitemaps.org/schemas/sitemap/0.9'

    doc = etree.ElementTree(urlset)

    for uri in urls:
        if uri is not None and (uri.startswith('https://purplepee.co') or uri.startswith('https://api.purplepee.co')):
            url = etree.SubElement(urlset, 'url')
            loc = etree.SubElement(url, 'loc')
            loc.text = uri

    doc.write(output, xml_declaration=True, encoding='utf-8', pretty_print=True)


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', help='set url to scan', type=str, required=True)
    parser.add_argument('--input', help='set sitemap file path', type=str)
    args = parser.parse_args()

    return args


def main():
    args = argparser()

    options = webdriver.ChromeOptions()
    options.binary_location = '/usr/bin/chromium-browser'
    options.add_argument('headless')
    options.add_argument('disable-infobars')
    options.add_argument('window-size=1200x800')
    options.add_argument('start-maximized')

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(5)

    try:
        driver.get(args.url)
    except TimeoutException:
        return

    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div#xhr')))

    urls = set()
    for i in driver.find_elements(By.XPATH, '//a'):
        urls.add(i.get_attribute('href'))

    driver.close()

    if args.input:
        sitemap = retrieve_sitemap(args.input)

        for site in sitemap:
            urls.add(site[0]['loc'])

    create_sitemap(urls, args.input)


if __name__ == '__main__':
    main()
