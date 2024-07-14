import argparse
import sys
import urllib
from datetime import datetime
from pathlib import Path

import requests
import structlog
from dateutil import tz

# TODO: config
TOKEN_PATH = Path("~/.github_token").expanduser()
GITHUB_USER = "lkirk"

URL_BASE = "https://api.github.com/users"


def parse_timestamp(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def read_github_token():
    with open(TOKEN_PATH) as fp:
        return fp.read().strip()


LOG = structlog.get_logger("github")


def get_most_recent_container_hash(container_url: str) -> str:
    package_name = container_url.replace(f"ghcr.io/{GITHUB_USER}/", "")
    token = read_github_token()
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = (
        f"{URL_BASE}/{GITHUB_USER}/packages/container"
        f"/{urllib.parse.quote_plus(package_name)}/versions"
    )

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    result = sorted(
        [(parse_timestamp(r["created_at"]), r["name"]) for r in response.json()],
        key=lambda k: k[0],
    )
    most_recent_time, most_recent_hash = result[-1]
    local_timezone = tz.tzlocal().tzname(datetime.now())

    LOG.info(
        "got recent container hash",
        container=f"{GITHUB_USER}/{package_name}",
        publish_time=f"{most_recent_time.astimezone(tz.tzlocal())} ({local_timezone})",
    )

    return most_recent_hash
