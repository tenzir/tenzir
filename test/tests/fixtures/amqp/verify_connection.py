# runner: python
"""Verify the RabbitMQ fixture starts correctly and accepts broker operations."""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


def _request_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(
        None,
        os.environ["AMQP_MANAGEMENT_URL"],
        os.environ["AMQP_USER"],
        os.environ["AMQP_PASSWORD"],
    )
    opener = urllib.request.build_opener(
        urllib.request.HTTPBasicAuthHandler(password_manager)
    )
    payload = None
    headers = {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{os.environ['AMQP_MANAGEMENT_URL']}{path}",
        data=payload,
        headers=headers,
        method=method,
    )
    with opener.open(request, timeout=5) as response:
        text = response.read().decode("utf-8")
        return json.loads(text) if text else {}


def main() -> None:
    overview = _request_json("GET", "/api/overview")
    listeners = overview.get("listeners", [])
    if not listeners:
        raise RuntimeError("RabbitMQ management API did not report listeners")
    print("rabbitmq-ready")


if __name__ == "__main__":
    main()
