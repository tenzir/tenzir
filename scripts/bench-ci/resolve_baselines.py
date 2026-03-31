#!/usr/bin/env python3
"""Resolve benchmark baseline refs for `main` and the latest stable release."""

from __future__ import annotations

import argparse
import json

from common import gh_api


def is_stable_release_tag(tag: str) -> bool:
    return "-rc" not in tag.lower()


def choose_latest_stable_release(releases: list[dict[str, object]]) -> str | None:
    for release in releases:
        tag_name = release.get("tag_name")
        if not isinstance(tag_name, str):
            continue
        if release.get("draft") or release.get("prerelease"):
            continue
        if not is_stable_release_tag(tag_name):
            continue
        return tag_name
    return None


def resolve_baselines(repo: str) -> dict[str, str | None]:
    commit = gh_api(f"repos/{repo}/commits/main")
    releases = gh_api(f"repos/{repo}/releases?per_page=50")
    if not isinstance(releases, list):
        raise RuntimeError("unexpected release response from GitHub API")
    main_sha = commit.get("sha")
    if not isinstance(main_sha, str):
        raise RuntimeError("missing main branch SHA")
    return {
        "main_sha": main_sha,
        "latest_stable_tag": choose_latest_stable_release(releases),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="tenzir/tenzir", help="GitHub repository")
    args = parser.parse_args()
    print(json.dumps(resolve_baselines(args.repo), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
