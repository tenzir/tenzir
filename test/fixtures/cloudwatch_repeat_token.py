"""Fake CloudWatch Logs fixture for repeated pagination tokens."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from tenzir_test import FixtureHandle, fixture

from ._utils import find_free_port

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CloudWatchRepeatTokenAssertions:
    request_count: int | None = None


def _extract_assertions(
    raw: CloudWatchRepeatTokenAssertions | dict[str, Any] | None,
) -> CloudWatchRepeatTokenAssertions:
    if raw is None:
        return CloudWatchRepeatTokenAssertions()
    if isinstance(raw, CloudWatchRepeatTokenAssertions):
        return raw
    if isinstance(raw, dict):
        return CloudWatchRepeatTokenAssertions(**raw)
    raise TypeError("cloudwatch_repeat_token fixture assertions must be a mapping")


@fixture(
    name="cloudwatch_repeat_token",
    log_teardown=True,
    assertions=CloudWatchRepeatTokenAssertions,
)
def run() -> FixtureHandle:
    requests: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode()
            payload = json.loads(body) if body else {}
            requests.append(payload)
            response = {
                "events": [],
                "nextToken": "repeat-token",
            }
            if len(requests) == 1:
                response["events"] = [
                    {
                        "timestamp": 1_700_000_000_000,
                        "ingestionTime": 1_700_000_000_001,
                        "logStreamName": "repeat-stream",
                        "message": '{"id":1,"msg":"first"}',
                        "eventId": "event-1",
                    }
                ]
            if len(requests) > 2:
                response.pop("nextToken")
            encoded = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/x-amz-json-1.1")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, fmt: str, *args: object) -> None:
            logger.debug("cloudwatch repeat-token: %s", fmt % args if args else fmt)

    port = find_free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def teardown() -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    def assert_test(
        *,
        test: Any,
        assertions: CloudWatchRepeatTokenAssertions | dict[str, Any] | None,
        **_: Any,
    ) -> None:
        del test
        expected = _extract_assertions(assertions)
        if (
            expected.request_count is not None
            and len(requests) != expected.request_count
        ):
            raise AssertionError(
                "expected "
                f"{expected.request_count} CloudWatch request(s), got {len(requests)}"
            )

    return FixtureHandle(
        env={
            "AWS_ENDPOINT_URL_LOGS": f"http://127.0.0.1:{port}",
            "AWS_EC2_METADATA_DISABLED": "true",
        },
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
