# runner: python

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime


def _timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _query(expr: str, timestamp: float) -> list[dict[str, object]]:
    base_url = os.environ["PROMETHEUS_URL"]
    params = urllib.parse.urlencode({"query": expr, "time": f"{timestamp:.3f}"})
    with urllib.request.urlopen(
        f"{base_url}/api/v1/query?{params}", timeout=5
    ) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload["status"] != "success":
        raise AssertionError(payload)
    return payload["data"]["result"]


def _wait_for_value(expr: str, timestamp: float, expected: float) -> None:
    deadline = time.monotonic() + 15
    last: object = None
    while time.monotonic() < deadline:
        last = _query(expr, timestamp)
        if len(last) == 1:
            value = float(last[0]["value"][1])
            if abs(value - expected) < 1e-9:
                return
        time.sleep(0.5)
    raise AssertionError(f"expected {expected} for {expr}, got {last}")


def main() -> None:
    _wait_for_value(
        'tenzir_prometheus_fixture_v1_total{source="container"}',
        _timestamp("2026-05-15T11:00:00Z"),
        42.0,
    )
    print("ok")


if __name__ == "__main__":
    main()
