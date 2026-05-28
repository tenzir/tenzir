"""OpenAI Responses API fixture for ai::prompt integration tests."""

from __future__ import annotations

import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator
from urllib.parse import urlsplit

from tenzir_test import fixture


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


def _make_handler(errors: list[str]):
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
            self._reply(
                HTTPStatus.OK,
                {
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
                },
            )

    return OpenAIResponsesHandler


@fixture(name="openai_responses")
def run() -> Iterator[dict[str, str]]:
    errors: list[str] = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(errors))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        port = server.server_address[1]
        yield {
            "OPENAI_RESPONSES_FIXTURE_ENDPOINT": f"http://127.0.0.1:{port}/v1",
        }
    finally:
        server.shutdown()
        worker.join()
        if errors:
            raise RuntimeError(errors[0])
