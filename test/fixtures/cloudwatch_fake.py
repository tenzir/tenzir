"""Scripted CloudWatch Logs fixture that captures the read requests.

The fixture serves `FilterLogEvents`, `GetLogEvents`, and `StartLiveTail` over
a small deterministic dataset, and records the operation and JSON body of
every request so that tests can assert what `from_amazon_cloudwatch` asked
for. It models the request contract, not CloudWatch's matching semantics:
filter patterns of quoted phrases match as case-sensitive substrings.

Environment variables yielded:
- AWS_ENDPOINT_URL_LOGS
- CLOUDWATCH_FAKE_GROUP_ARN
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import zlib
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from tenzir_test import FixtureHandle, current_options, fixture

logger = logging.getLogger(__name__)

_HOST = "127.0.0.1"
_GROUP_ARN = "arn:aws:logs:us-east-1:123456789012:log-group:/tenzir/app"
_BASE_MS = 1_700_000_000_000


def _default_events() -> list[dict[str, Any]]:
    messages = [
        ("app-1", "INFO service started"),
        ("app-2", "ERROR disk full"),
        ("app-1", "WARN disk at 90%"),
        ("app-2", "ERROR request timeout"),
        ("app-1", "ERROR connection refused"),
        ("app-3", "INFO service stopped"),
        ("app-1", 'ERROR quoted "value"'),
    ]
    return [
        {
            "timestamp": _BASE_MS + index * 1000,
            "ingestionTime": _BASE_MS + index * 1000 + 1,
            "logStreamName": stream,
            "message": message,
            "eventId": f"event-{index}",
        }
        for index, (stream, message) in enumerate(messages)
    ]


@dataclass(frozen=True)
class CloudWatchFakeOptions:
    # The maximum number of events per page, independent of `limit`.
    page_size: int = 100
    # Phrases that make a filter pattern fail with `InvalidParameterException`.
    reject_phrases: list[str] = field(default_factory=list)
    # Whether Live Tail marks its update as sampled.
    sampled: bool = False


@dataclass(frozen=True)
class CloudWatchFakeAssertions:
    # The exact requests, in order. Each entry names the `operation` and the
    # fields of the JSON body; a field with value `null` must be absent.
    requests: list[dict[str, Any]] | None = None
    # The first requests, in order, for reads that a downstream stop ends.
    request_prefix: list[dict[str, Any]] | None = None


def _matches_pattern(pattern: str | None, message: str) -> bool:
    if not pattern:
        return True
    terms = re.findall(r'([-?]?)"([^"]*)"', pattern)
    required = [phrase for sign, phrase in terms if sign == ""]
    excluded = [phrase for sign, phrase in terms if sign == "-"]
    optional = [phrase for sign, phrase in terms if sign == "?"]
    if any(phrase not in message for phrase in required):
        return False
    if any(phrase in message for phrase in excluded):
        return False
    if optional and not any(phrase in message for phrase in optional):
        return False
    return True


def _header(name: str, value: str) -> bytes:
    encoded_name = name.encode()
    encoded_value = value.encode()
    return (
        bytes([len(encoded_name)])
        + encoded_name
        + bytes([7])
        + len(encoded_value).to_bytes(2, "big")
        + encoded_value
    )


def _event_stream_message(event_type: str, payload: dict[str, Any]) -> bytes:
    """Encodes an event of the AWS event stream protocol."""
    headers = (
        _header(":message-type", "event")
        + _header(":event-type", event_type)
        + _header(":content-type", "application/json")
    )
    body = json.dumps(payload).encode()
    total = 12 + len(headers) + len(body) + 4
    prelude = total.to_bytes(4, "big") + len(headers).to_bytes(4, "big")
    message = prelude + zlib.crc32(prelude).to_bytes(4, "big") + headers + body
    return message + zlib.crc32(message).to_bytes(4, "big")


def _matches_request(expected: dict[str, Any], actual: dict[str, Any]) -> bool:
    for key, value in expected.items():
        if key == "operation":
            if actual["operation"] != value:
                return False
            continue
        body = actual["body"]
        if value is None:
            if key in body:
                return False
        elif body.get(key) != value:
            return False
    return True


@fixture(
    name="cloudwatch_fake",
    options=CloudWatchFakeOptions,
    assertions=CloudWatchFakeAssertions,
)
def run() -> FixtureHandle:
    options = current_options("cloudwatch_fake")
    if not isinstance(options, CloudWatchFakeOptions):
        options = CloudWatchFakeOptions()
    events = _default_events()
    requests: list[dict[str, Any]] = []
    lock = threading.Lock()
    stopping = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def handle(self) -> None:
            # The operator drops its connection when the pipeline ends.
            try:
                super().handle()
            except (BrokenPipeError, ConnectionResetError):
                return

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode()
            body = json.loads(raw) if raw else {}
            target = self.headers.get("X-Amz-Target", "")
            operation = target.rsplit(".", 1)[-1]
            with lock:
                requests.append({"operation": operation, "body": body})
            pattern = body.get("filterPattern") or body.get("logEventFilterPattern")
            if pattern and any(p in pattern for p in options.reject_phrases):
                self._json(
                    400,
                    {
                        "__type": "InvalidParameterException",
                        "message": "Invalid filter pattern",
                    },
                )
                return
            if operation == "FilterLogEvents":
                self._filter_log_events(body)
            elif operation == "GetLogEvents":
                self._get_log_events(body)
            elif operation == "StartLiveTail":
                self._start_live_tail(body)
            else:
                self._json(400, {"__type": "UnknownOperationException"})

        def _json(self, code: int, obj: object) -> None:
            encoded = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/x-amz-json-1.1")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _page(
            self, matching: list[dict[str, Any]], body: dict[str, Any], token: str
        ) -> tuple[list[dict[str, Any]], int]:
            offset = int(token.removeprefix("token-")) if token else 0
            size = min(body.get("limit", 10_000), options.page_size)
            return matching[offset : offset + size], offset + size

        def _filter_log_events(self, body: dict[str, Any]) -> None:
            streams = body.get("logStreamNames")
            prefix = body.get("logStreamNamePrefix")
            matching = [
                event
                for event in events
                if (streams is None or event["logStreamName"] in streams)
                and (prefix is None or event["logStreamName"].startswith(prefix))
                and event["timestamp"] >= body.get("startTime", 0)
                and event["timestamp"] <= body.get("endTime", 2**63)
                and _matches_pattern(body.get("filterPattern"), event["message"])
            ]
            page, end = self._page(matching, body, body.get("nextToken", ""))
            response: dict[str, Any] = {"events": page}
            if end < len(matching):
                response["nextToken"] = f"token-{end}"
            self._json(200, response)

        def _get_log_events(self, body: dict[str, Any]) -> None:
            matching = [
                {k: v for k, v in event.items() if k != "logStreamName"}
                for event in events
                if event["logStreamName"] == body.get("logStreamName")
                and event["timestamp"] >= body.get("startTime", 0)
                and event["timestamp"] < body.get("endTime", 2**63)
            ]
            token = body.get("nextToken", "")
            page, end = self._page(matching, body, token)
            # Like CloudWatch, return the token of the position after the
            # page, which repeats the given token once the stream is
            # exhausted, so that the read ends.
            next_token = f"token-{min(end, len(matching))}"
            self._json(
                200,
                {
                    "events": page,
                    "nextForwardToken": next_token,
                    "nextBackwardToken": next_token,
                },
            )

        def _start_live_tail(self, body: dict[str, Any]) -> None:
            pattern = body.get("logEventFilterPattern")
            results = [
                {
                    "ingestionTime": event["ingestionTime"],
                    "logGroupIdentifier": _GROUP_ARN,
                    "logStreamName": event["logStreamName"],
                    "message": event["message"],
                    "timestamp": event["timestamp"],
                }
                for event in events
                if _matches_pattern(pattern, event["message"])
            ]
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.amazon.eventstream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            frames = [
                _event_stream_message(
                    "sessionStart",
                    {
                        "requestId": "request-1",
                        "sessionId": "session-1",
                        "logGroupIdentifiers": body.get("logGroupIdentifiers", []),
                        "logEventFilterPattern": pattern or "",
                    },
                ),
                _event_stream_message(
                    "sessionUpdate",
                    {
                        "sessionMetadata": {"sampled": options.sampled},
                        "sessionResults": results,
                    },
                ),
            ]
            try:
                for frame in frames:
                    self.wfile.write(f"{len(frame):x}\r\n".encode() + frame + b"\r\n")
                    self.wfile.flush()
                # Keep the session open until the pipeline ends, so that the
                # operator does not start another one.
                stopping.wait(timeout=30)
                self.wfile.write(b"0\r\n\r\n")
            except (BrokenPipeError, ConnectionResetError):
                return

        def log_message(self, fmt: str, *args: object) -> None:
            logger.debug("cloudwatch fake: %s", fmt % args if args else fmt)

    server = ThreadingHTTPServer((_HOST, 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def teardown() -> None:
        stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    def assert_test(
        *, assertions: CloudWatchFakeAssertions | dict[str, Any], **_: Any
    ) -> None:
        if not isinstance(assertions, CloudWatchFakeAssertions):
            assert not assertions, assertions
            return
        # Give a downstream stop time to settle before reading the log.
        time.sleep(0.1)
        with lock:
            captured = list(requests)
        if assertions.requests is not None:
            assert len(captured) == len(assertions.requests), captured
            for expected, actual in zip(assertions.requests, captured):
                assert _matches_request(expected, actual), (expected, actual)
        if assertions.request_prefix is not None:
            assert len(captured) >= len(assertions.request_prefix), captured
            for expected, actual in zip(assertions.request_prefix, captured):
                assert _matches_request(expected, actual), (expected, actual)

    return FixtureHandle(
        env={
            "AWS_ENDPOINT_URL_LOGS": f"http://{_HOST}:{server.server_port}",
            "AWS_EC2_METADATA_DISABLED": "true",
            "CLOUDWATCH_FAKE_GROUP_ARN": _GROUP_ARN,
        },
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
