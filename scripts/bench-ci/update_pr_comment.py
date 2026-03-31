#!/usr/bin/env python3
"""Create or update the sticky benchmark comment on a pull request."""

from __future__ import annotations

import argparse
import os

from common import COMMENT_MARKER, gh_api


def wrap_comment_body(body: str) -> str:
    rendered = body.strip()
    return f"{COMMENT_MARKER}\n{rendered}\n"


def current_authenticated_login() -> str:
    configured = os.environ.get("BENCHMARK_COMMENT_AUTHOR_LOGIN", "").strip()
    if configured:
        return configured
    payload = gh_api("user")
    if not isinstance(payload, dict):
        raise RuntimeError("unexpected authenticated user response")
    login = payload.get("login")
    if not isinstance(login, str) or not login:
        raise RuntimeError("authenticated user response is missing a login")
    return login


def select_existing_comment(
    comments: list[dict[str, object]],
    *,
    author_login: str,
) -> dict[str, object] | None:
    for comment in comments:
        body = comment.get("body")
        user = comment.get("user")
        login = user.get("login") if isinstance(user, dict) else None
        if isinstance(body, str) and COMMENT_MARKER in body and login == author_login:
            return comment
    return None


def update_pr_comment(repo: str, pr_number: int, body: str) -> None:
    wrapped = wrap_comment_body(body)
    author_login = current_authenticated_login()
    comments = gh_api(
        f"repos/{repo}/issues/{pr_number}/comments?per_page=100",
        paginate=True,
    )
    if not isinstance(comments, list):
        raise RuntimeError("unexpected pull request comments response")
    existing = select_existing_comment(comments, author_login=author_login)
    if existing is None:
        gh_api(
            f"repos/{repo}/issues/{pr_number}/comments",
            method="POST",
            payload={"body": wrapped},
        )
        return
    comment_id = existing.get("id")
    if not isinstance(comment_id, int):
        raise RuntimeError("existing comment is missing an id")
    gh_api(
        f"repos/{repo}/issues/comments/{comment_id}",
        method="PATCH",
        payload={"body": wrapped},
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="tenzir/tenzir", help="GitHub repository")
    parser.add_argument("--pr-number", type=int, required=True, help="Pull request number")
    parser.add_argument("--body-file", required=True, help="Path to the rendered markdown body")
    args = parser.parse_args()
    with open(args.body_file, encoding="utf-8") as handle:
        body = handle.read()
    update_pr_comment(args.repo, args.pr_number, body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
