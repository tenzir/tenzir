"""Splunk fixture for to_splunk integration testing.

Provides a Splunk single-node container with HTTP Event Collector enabled.

Environment variables yielded:
- SPLUNK_HEC_URL: HEC endpoint base URL.
- SPLUNK_HEC_TOKEN: HEC token configured in the container.
- SPLUNK_INDEX: Index to write test events to.
- SPLUNK_MGMT_URL: HTTPS management API base URL.
- SPLUNK_ADMIN_USER: Management API username.
- SPLUNK_ADMIN_PASSWORD: Management API password.

Assertions payload accepted under ``assertions.fixtures.splunk``:
- count: optional number of search results expected.
- contains: substring or list of substrings expected in matching events.
- index: Splunk index to search, defaults to SPLUNK_INDEX.
- source: optional source filter.
- sourcetype: optional sourcetype filter.
- host: optional host filter.
- search: optional full Splunk search query, overriding generated filters.
"""

from __future__ import annotations

import base64
import json
import logging
import shutil
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable
from tenzir_test.fixtures.container_runtime import (
    ContainerReadinessTimeout,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
    wait_until_ready,
)

from ._utils import find_free_port

logger = logging.getLogger(__name__)

SPLUNK_IMAGE = "docker.io/splunk/splunk:latest"
SPLUNK_HEC_TOKEN = "abcd1234"
SPLUNK_PASSWORD = "tenzir123"
SPLUNK_USER = "admin"
SPLUNK_INDEX = "main"
STARTUP_TIMEOUT = 180
HEALTH_CHECK_INTERVAL = 2
ASSERTION_TIMEOUT = 60
ASSERTION_INTERVAL = 1


@dataclass(frozen=True)
class SplunkAssertions:
    count: int | None = None
    contains: list[str] = field(default_factory=list)
    index: str = SPLUNK_INDEX
    source: str | None = None
    sourcetype: str | None = None
    host: str | None = None
    search: str | None = None

    def __post_init__(self) -> None:
        contains = self.contains
        if isinstance(contains, str):
            object.__setattr__(self, "contains", [contains])


def _extract_assertions(raw: SplunkAssertions | dict[str, Any]) -> SplunkAssertions:
    if isinstance(raw, SplunkAssertions):
        return raw
    if isinstance(raw, dict):
        return SplunkAssertions(**raw)
    raise TypeError("splunk fixture assertions must be a mapping")


def _ssl_context() -> ssl.SSLContext:
    return ssl._create_unverified_context()  # noqa: S323


def _basic_auth_header() -> str:
    raw = f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()
    return "Basic " + base64.b64encode(raw).decode()


def _urlopen(
    request: urllib.request.Request,
    *,
    timeout: float = 5,
) -> bytes:
    with urllib.request.urlopen(
        request, timeout=timeout, context=_ssl_context()
    ) as response:
        return response.read()


def _start_splunk(
    runtime: RuntimeSpec,
    *,
    hec_port: int,
    mgmt_port: int,
    defaults_path: Path,
) -> ManagedContainer:
    container_name = f"tenzir-test-splunk-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "--platform",
        "linux/amd64",
        "-p",
        f"{hec_port}:8088",
        "-p",
        f"{mgmt_port}:8089",
        "-v",
        f"{defaults_path}:/tmp/defaults/default.yml:ro",
        "-e",
        "SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com",
        "-e",
        "SPLUNK_START_ARGS=--accept-license",
        "-e",
        f"SPLUNK_HEC_TOKEN={SPLUNK_HEC_TOKEN}",
        "-e",
        f"SPLUNK_PASSWORD={SPLUNK_PASSWORD}",
        SPLUNK_IMAGE,
    ]
    logger.info("Starting Splunk container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("Splunk container started: %s", container.container_id[:12])
    return container


def _stop_splunk(container: ManagedContainer) -> None:
    logger.info("Stopping Splunk container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop Splunk container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_splunk(hec_port: int, mgmt_port: int) -> None:
    def _probe() -> tuple[bool, dict[str, str]]:
        management_url = (
            f"https://127.0.0.1:{mgmt_port}/services/server/info?output_mode=json"
        )
        management_request = urllib.request.Request(
            management_url,
            headers={"Authorization": _basic_auth_header()},
        )
        hec_url = f"http://127.0.0.1:{hec_port}/services/collector/health"
        hec_request = urllib.request.Request(
            hec_url,
            headers={"Authorization": f"Splunk {SPLUNK_HEC_TOKEN}"},
        )
        try:
            _urlopen(management_request, timeout=3)
            _urlopen(hec_request, timeout=3)
            return True, {"management": "ready", "hec": "ready"}
        except (urllib.error.URLError, OSError) as exc:
            return False, {"error": str(exc)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=STARTUP_TIMEOUT,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="Splunk startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Splunk is ready")


def _quote_search_value(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _make_search(assertions: SplunkAssertions) -> str:
    if assertions.search:
        return assertions.search
    terms = [f"index={_quote_search_value(assertions.index)}"]
    if assertions.source:
        terms.append(f"source={_quote_search_value(assertions.source)}")
    if assertions.sourcetype:
        terms.append(f"sourcetype={_quote_search_value(assertions.sourcetype)}")
    if assertions.host:
        terms.append(f"host={_quote_search_value(assertions.host)}")
    return "search " + " ".join(terms)


def _run_search(mgmt_port: int, search: str) -> list[dict[str, Any]]:
    url = f"https://127.0.0.1:{mgmt_port}/services/search/jobs/export"
    payload = urllib.parse.urlencode(
        {
            "search": search,
            "output_mode": "json",
            "earliest_time": "-10m",
            "latest_time": "now",
        }
    ).encode()
    request = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": _basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        body = _urlopen(request, timeout=10).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Splunk search failed with HTTP {exc.code}: {detail}")
    results: list[dict[str, Any]] = []
    for line in body.splitlines():
        if not line:
            continue
        event = json.loads(line)
        result = event.get("result")
        if isinstance(result, dict):
            results.append(result)
    return results


def _result_text(result: dict[str, Any]) -> str:
    return json.dumps(result, sort_keys=True, separators=(",", ":"))


def _verify_search(
    *,
    test: Path,
    mgmt_port: int,
    assertions: SplunkAssertions,
) -> None:
    search = _make_search(assertions)
    deadline = time.monotonic() + ASSERTION_TIMEOUT
    last_results: list[dict[str, Any]] = []
    last_error: str | None = None
    while True:
        search_failed = False
        try:
            results = _run_search(mgmt_port, search)
            last_results = results
            last_error = None
            result_texts = [_result_text(result) for result in results]
        except RuntimeError as exc:
            results = []
            result_texts = []
            last_error = str(exc)
            search_failed = True
        all_contains = all(
            any(needle in text for text in result_texts)
            for needle in assertions.contains
        )
        count_matches = assertions.count is None or len(results) == assertions.count
        if not search_failed and count_matches and all_contains:
            return
        if time.monotonic() >= deadline:
            details = "; ".join(result_texts) or "no results"
            if last_error:
                details = f"{details}; last search error: {last_error}"
            expected_count = (
                "any" if assertions.count is None else str(assertions.count)
            )
            raise AssertionError(
                f"{test.name}: Splunk search did not match {search!r}: "
                f"expected count={expected_count}, contains={assertions.contains}, "
                f"got {len(last_results)} result(s): {details}"
            )
        time.sleep(ASSERTION_INTERVAL)


@fixture(assertions=SplunkAssertions)
def splunk() -> FixtureHandle:
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )

    hec_port = find_free_port()
    mgmt_port = find_free_port()
    container: ManagedContainer | None = None
    temp_dir = Path(tempfile.mkdtemp(prefix="splunk-fixture-"))
    defaults_path = temp_dir / "default.yml"
    defaults_path.write_text(
        "\n".join(
            [
                "splunk:",
                "  hec:",
                "    enable: true",
                "    ssl: false",
                "    port: 8088",
                f"    token: {SPLUNK_HEC_TOKEN}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    try:
        container = _start_splunk(
            runtime,
            hec_port=hec_port,
            mgmt_port=mgmt_port,
            defaults_path=defaults_path,
        )
        _wait_for_splunk(hec_port, mgmt_port)
    except Exception:
        if container is not None:
            _stop_splunk(container)
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    assert container is not None

    def _assert_test(
        *,
        test: Path,
        assertions: SplunkAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        assertion_config = _extract_assertions(assertions)
        _verify_search(
            test=test,
            mgmt_port=mgmt_port,
            assertions=assertion_config,
        )

    def _teardown() -> None:
        try:
            _stop_splunk(container)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    return FixtureHandle(
        env={
            "SPLUNK_HEC_URL": f"http://127.0.0.1:{hec_port}",
            "SPLUNK_HEC_TOKEN": SPLUNK_HEC_TOKEN,
            "SPLUNK_INDEX": SPLUNK_INDEX,
            "SPLUNK_MGMT_URL": f"https://127.0.0.1:{mgmt_port}",
            "SPLUNK_ADMIN_USER": SPLUNK_USER,
            "SPLUNK_ADMIN_PASSWORD": SPLUNK_PASSWORD,
        },
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
