#!/usr/bin/env python3

import argparse

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import JavascriptException
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

from lxml import etree


class SitemapGenerator:
    """Generate sitemap files by crawling a target URL."""

    @staticmethod
    def load_sitemap(filename: str) -> str:
        with open(filename, 'r') as handle:
            return handle.read()

    @classmethod
    def retrieve_sitemap(cls, input_path: str) -> list[str]:
        xml = cls.load_sitemap(input_path)
        root = etree.XML(xml.encode())
        sitemap: list[str] = []

        for element in root:
            children = element.getchildren()
            sitemap.append(children[0].text)

        return sitemap

    @staticmethod
    def create_sitemap(urls: set[str], output: str) -> None:
        urlset = etree.Element('urlset')
        urlset.attrib['xmlns'] = 'http://www.sitemaps.org/schemas/sitemap/0.9'

        doc = etree.ElementTree(urlset)

        for uri in urls:
            if uri is not None and (
                uri.startswith('https://purplepee.co')
                or uri.startswith('https://api.purplepee.co')
            ):
                url = etree.SubElement(urlset, 'url')
                loc = etree.SubElement(url, 'loc')
                loc.text = uri

        doc.write(output, xml_declaration=True, encoding='utf-8', pretty_print=True)

    @staticmethod
    def build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument('--url', help='set url to scan', type=str, required=True)
        parser.add_argument('--input', help='set sitemap file path', type=str)
        return parser

    @staticmethod
    def configure_driver() -> webdriver.Chrome:
        options = webdriver.ChromeOptions()
        options.binary_location = '/usr/bin/chromium-browser'
        options.add_argument('headless')
        options.add_argument('disable-infobars')
        options.add_argument('window-size=1200x800')
        options.add_argument('start-maximized')

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(5)
        return driver

    @classmethod
    def run(cls) -> None:
        args = cls.build_parser().parse_args()

        driver = cls.configure_driver()

        try:
            driver.get(args.url)
        except TimeoutException:
            return

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div#xhr'))
        )

        urls: set[str] = set()
        for link in driver.find_elements(By.XPATH, '//a'):
            urls.add(link.get_attribute('href'))

        driver.close()

        if args.input:
            sitemap = cls.retrieve_sitemap(args.input)
            urls.update(sitemap)

            cls.create_sitemap(urls, args.input)


def main():
    SitemapGenerator.run()


if __name__ == '__main__':
    CLITool(main).run()
