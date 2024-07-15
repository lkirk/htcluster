#!/usr/bin/env python

import argparse
import json
import sys
import textwrap
import urllib.parse
from datetime import datetime
from typing import Optional

import requests
from dateutil import tz

from htcluster.config import Config, load_config

GITHUB_API_URL_BASE = "https://api.github.com"
DOCKER_API_URL_BASE = "https://ghcr.io/v2"


def parse_timestamp(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("github_user", help="github user name")
    parser.add_argument("package_name", help="name of package to pull from")
    parser.add_argument(
        "-r", "--raw", help="print the raw hash", action="store_true", default=False
    )
    parser.add_argument(
        "-n",
        "--no-print",
        help="if printing raw, do not print publish time",
        action="store_true",
        default=False,
    )
    return parser.parse_args()


def load_tokens(config: Config) -> tuple[str, str]:
    with open(config.github_token) as fp:
        github_token = fp.read().strip()

    with open(config.docker_token) as fp:
        docker_token = json.load(fp)["auths"]["ghcr.io"]["auth"]

    return github_token, docker_token


def github_headers(github_token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def docker_headers(docker_token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.docker.distribution.manifest.v2+json",
        "Authorization": f"Bearer {docker_token}",
    }


def github_request(github_token: str, url: str) -> dict:
    headers = github_headers(github_token)
    url = f"{GITHUB_API_URL_BASE}/{url}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def docker_request(docker_token: str, url: str) -> dict:
    headers = docker_headers(docker_token)
    url = f"{DOCKER_API_URL_BASE}/{url}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_container_hash(
    github_token: str, github_user: str, package_name: str
) -> tuple[datetime, str]:
    quoted_package_name = urllib.parse.quote_plus(package_name)
    url = f"users/{github_user}/packages/container/{quoted_package_name}/versions"
    response = github_request(github_token, url)
    result = sorted(
        [(parse_timestamp(r["created_at"]), r["name"]) for r in response],
        key=lambda k: k[0],
    )
    publish_time, most_recent_hash = result[-1]
    return publish_time, most_recent_hash


def get_container_commit(
    docker_token: str, github_user: str, package_name: str, container_hash: str
) -> str:
    # First get the blob sha of the container config
    manifest_url = f"{github_user}/{package_name}/manifests/{container_hash}"
    config_digest = docker_request(docker_token, manifest_url)["config"]["digest"]

    # Then query this blob to get container config
    blob_url = f"{github_user}/{package_name}/blobs/{config_digest}"
    return docker_request(docker_token, blob_url)["config"]["Labels"][
        "org.opencontainers.image.revision"
    ]


def get_commit_info(
    github_token: str, github_user: str, package_name: str, commit_hash: str
) -> tuple[datetime, str]:
    repo_name = package_name.split("/")[0]
    url = f"repos/{github_user}/{repo_name}/commits/{commit_hash}"
    response = github_request(github_token, url)
    commit_timestamp = parse_timestamp(response["commit"]["committer"]["date"])
    return commit_timestamp, response["commit"]["message"]


def print_result(
    args: argparse.Namespace,
    container_hash: str,
    publish_time: Optional[datetime],
    commit_time: Optional[datetime],
    commit_message: Optional[str],
) -> None:
    local_timezone = tz.tzlocal().tzname(datetime.now())

    if args.raw:
        if not args.no_print:
            assert publish_time is not None  # mypy
            assert commit_time is not None  # mypy
            assert commit_message is not None  # mypy
            print(
                textwrap.dedent(
                    f"""\
                    Container Published at: {publish_time.astimezone(tz.tzlocal())} ({local_timezone})",
                    Committed at: {commit_time.astimezone(tz.tzlocal())} ({local_timezone})
                    Commit message: {commit_message}
                    """
                ),
                file=sys.stderr,
            )
        print(container_hash)
    else:
        assert publish_time is not None  # mypy
        assert commit_time is not None  # mypy
        assert commit_message is not None  # mypy
        print(
            textwrap.dedent(
                f"""\
                Most recent container for {args.github_user}/{args.package_name}:

                Published at: {publish_time.astimezone(tz.tzlocal())} ({local_timezone})
                Committed at: {commit_time.astimezone(tz.tzlocal())} ({local_timezone})
                Commit message: {commit_message}
            
                {container_hash}\
            """
            )
        )


def main():
    args = parse_args()
    config = load_config()
    github_token, docker_token = load_tokens(config)
    publish_time, container_hash = get_container_hash(
        github_token, args.github_user, args.package_name
    )
    commit_time, commit_message = None, None
    if not (args.raw and args.no_print):
        commit_hash = get_container_commit(
            docker_token, args.github_user, args.package_name, container_hash
        )
        commit_time, commit_message = get_commit_info(
            github_token, args.github_user, args.package_name, commit_hash
        )
    print_result(args, container_hash, publish_time, commit_time, commit_message)


if __name__ == "__main__":
    main()
