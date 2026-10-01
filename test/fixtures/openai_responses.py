"""OpenAI Responses API fixture for ai_prompt integration tests."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from tenzir_test import FixtureHandle, fixture


@dataclass(frozen=True)
class OpenAIResponsesAssertions:
    inputs: list[str] | None = None
    body: dict[str, object] | None = None
    authorization: str | None = None


def _read_payload(raw_input: object) -> object:
    if not isinstance(raw_input, str):
        return raw_input
    try:
        return json.loads(raw_input)
    except json.JSONDecodeError:
        return raw_input


def _make_output_text(payload: object, raw_input: object) -> str:
    if not isinstance(payload, dict):
        return f"echo:{raw_input}"
    mode = payload.get("mode")
    if mode == "json":
        return json.dumps(
            {
                "answer": 42,
                "id": payload.get("id"),
            },
            separators=(",", ":"),
        )
    if mode == "json_number":
        return "42"
    if mode == "order":
        return f"order:{payload.get('id')}"
    if "message" in payload:
        return f"id={payload.get('id')} message={payload.get('message')}"
    return f"echo:{raw_input}"


def _make_handler(
    errors: list[str], requests: list[tuple[dict[str, object], str | None]]
):
    class OpenAIResponsesHandler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _reply(self, status: HTTPStatus, payload: dict[str, object]) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> bytes:
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                content_length = 0
            if content_length <= 0:
                return b""
            return self.rfile.read(content_length) or b""

        def do_POST(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            body = self._read_body()
            if path != "/v1/responses":
                errors.append(f"expected path /v1/responses, got {path}")
                self._reply(HTTPStatus.NOT_FOUND, {"error": "not-found"})
                return
            content_type = self.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                errors.append(
                    f"expected application/json Content-Type, got {content_type}"
                )
            try:
                request = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError as error:
                errors.append(f"invalid request JSON: {error}")
                self._reply(HTTPStatus.BAD_REQUEST, {"error": "invalid-json"})
                return
            requests.append((request, self.headers.get("Authorization")))
            raw_input = request.get("input", "")
            payload = _read_payload(raw_input)
            if isinstance(payload, dict):
                delay_ms = payload.get("delay_ms", 0)
                try:
                    delay = max(0, int(delay_ms)) / 1000.0
                except (TypeError, ValueError):
                    delay = 0.0
                if delay:
                    time.sleep(delay)
                if payload.get("mode") == "fail":
                    self._reply(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        {"error": "fixture failure"},
                    )
                    return
            text = _make_output_text(payload, raw_input)
            response = {
                "id": "resp_fixture",
                "object": "response",
                "status": "completed",
                "model": request.get("model"),
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": text}],
                    }
                ],
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 2,
                    "total_tokens": 3,
                },
            }
            if isinstance(payload, dict):
                if payload.get("mode") == "missing_metadata":
                    del response["model"]
                    del response["usage"]
                elif payload.get("mode") == "partial_usage":
                    response["usage"] = {"input_tokens": 1}
            self._reply(HTTPStatus.OK, response)

    return OpenAIResponsesHandler


@fixture(name="openai_responses", assertions=OpenAIResponsesAssertions)
def run() -> FixtureHandle:
    errors: list[str] = []
    requests: list[tuple[dict[str, object], str | None]] = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(errors, requests))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    port = server.server_address[1]

    def _assert_test(
        *, assertions: OpenAIResponsesAssertions | dict[str, object], **_: object
    ) -> None:
        if isinstance(assertions, dict):
            assertions = OpenAIResponsesAssertions(**assertions)
        if assertions.inputs is not None:
            actual = [body.get("input") for body, _ in requests]
            if not all(isinstance(value, str) for value in actual) or sorted(
                actual
            ) != sorted(assertions.inputs):
                raise AssertionError(
                    f"expected request inputs {assertions.inputs!r}, got {actual!r}"
                )
        for body, authorization in requests:
            if authorization != assertions.authorization:
                raise AssertionError(
                    f"expected authorization {assertions.authorization!r}, "
                    f"got {authorization!r}"
                )
            if assertions.body is not None:
                actual = {key: value for key, value in body.items() if key != "input"}
                if actual != assertions.body:
                    raise AssertionError(
                        f"expected request body {assertions.body!r}, got {actual!r}"
                    )

    def _teardown() -> None:
        server.shutdown()
        worker.join()
        server.server_close()
        if errors:
            raise RuntimeError(errors[0])

    return FixtureHandle(
        env={"OPENAI_RESPONSES_FIXTURE_ENDPOINT": f"http://127.0.0.1:{port}/v1"},
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
