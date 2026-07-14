"""Mock ingestion server for to_google_secops integration tests."""

from __future__ import annotations

import base64
import gzip
import json
import os
import re
import tempfile
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Iterator
from urllib.parse import parse_qs

from tenzir_test import fixture

_HOST = "127.0.0.1"
_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_INGESTION_SCOPE = "https://www.googleapis.com/auth/malachite-ingestion"
_TOKENS_BY_SCOPE = {
    _CLOUD_PLATFORM_SCOPE: "test-import-token-12345",
    _INGESTION_SCOPE: "test-ingestion-token-12345",
}
_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCltiF2oP3KJJ+S
tTc1McylY+TuAi3AdohX7mmqIjd8a3eBYDHs7FlnUrFC4CRijCr0rUqYfg2pmk4a
6TaKbQRAhWDJ7XD931g7EBvCtd8+JQBNWVKnP9ByJUaO0hWVniM50KTsWtyX3up/
fS0W2R8Cyx4yvasE8QHH8gnNGtr94iiORDC7De2BwHi/iU8FxMVJAIyDLNfyk0hN
eheYKfIDBgJV2v6VaCOGWaZyEuD0FJ6wFeLybFBwibrLIBE5Y/StCrZoVZ5LocFP
T4o8kT7bU6yonudSCyNMedYmqHj/iF8B2UN1WrYx8zvoDqZk0nxIglmEYKn/6U7U
gyETGcW9AgMBAAECggEAC231vmkpwA7JG9UYbviVmSW79UecsLzsOAZnbtbn1VLT
Pg7sup7tprD/LXHoyIxK7S/jqINvPU65iuUhgCg3Rhz8+UiBhd0pCH/arlIdiPuD
2xHpX8RIxAq6pGCsoPJ0kwkHSw8UTnxPV8ZCPSRyHV71oQHQgSl/WjNhRi6PQroB
Sqc/pS1m09cTwyKQIopBBVayRzmI2BtBxyhQp9I8t5b7PYkEZDQlbdq0j5Xipoov
9EW0+Zvkh1FGNig8IJ9Wp+SZi3rd7KLpkyKPY7BK/g0nXBkDxn019cET0SdJOHQG
DiHiv4yTRsDCHZhtEbAMKZEpku4WxtQ+JjR31l8ueQKBgQDkO2oC8gi6vQDcx/CX
Z23x2ZUyar6i0BQ8eJFAEN+IiUapEeCVazuxJSt4RjYfwSa/p117jdZGEWD0GxMC
+iAXlc5LlrrWs4MWUc0AHTgXna28/vii3ltcsI0AjWMqaybhBTTNbMFa2/fV2OX2
UimuFyBWbzVc3Zb9KAG4Y7OmJQKBgQC5324IjXPq5oH8UWZTdJPuO2cgRsvKmR/r
9zl4loRjkS7FiOMfzAgUiXfH9XCnvwXMqJpuMw2PEUjUT+OyWjJONEK4qGFJkbN5
3ykc7p5V7iPPc7Zxj4mFvJ1xjkcj+i5LY8Me+gL5mGIrJ2j8hbuv7f+PWIauyjnp
Nx/0GVFRuQKBgGNT4D1L7LSokPmFIpYh811wHliE0Fa3TDdNGZnSPhaD9/aYyy78
LkxYKuT7WY7UVvLN+gdNoVV5NsLGDa4cAV+CWPfYr5PFKGXMT/Wewcy1WOmJ5des
AgMC6zq0TdYmMBN6WpKUpEnQtbmh3eMnuvADLJWxbH3wCkg+4xDGg2bpAoGAYRNk
MGtQQzqoYNNSkfus1xuHPMA8508Z8O9pwKU795R3zQs1NAInpjI1sOVrNPD7Ymwc
W7mmNzZbxycCUL/yzg1VW4P1a6sBBYGbw1SMtWxun4ZbnuvMc2CTCh+43/1l+FHe
Mmt46kq/2rH2jwx5feTbOE6P6PINVNRJh/9BDWECgYEAsCWcH9D3cI/QDeLG1ao7
rE2NcknP8N783edM07Z/zxWsIsXhBPY3gjHVz2LDl+QHgPWhGML62M0ja/6SsJW3
YvLLIc82V7eqcVJTZtaFkuht68qu/Jn1ezbzJMJ4YXDYo1+KFi+2CAGR06QILb+I
lUtj+/nH3HDQjM4ltYfTPUg=
-----END PRIVATE KEY-----
"""
_LOGS_PATH = re.compile(
    r"^/v1/projects/[^/]+/locations/[^/]+/instances/[^/]+"
    r"/logTypes/[^/]+/logs:import$"
)
_EVENTS_PATH = re.compile(
    r"^/v1/projects/[^/]+/locations/[^/]+/instances/[^/]+/events:import$"
)
_ENTITIES_PATH = re.compile(
    r"^/v1/projects/[^/]+/locations/[^/]+/instances/[^/]+/entities:import$"
)
_INGESTION_PATH = "/v2/unstructuredlogentries:batchCreate"


def _service_credentials(token_uri: str) -> str:
    return json.dumps(
        {
            "type": "service_account",
            "project_id": "test-only-project-id",
            "private_key_id": "a1a111aa1111a11a11a11aa111a111a1a1111111",
            "private_key": _PRIVATE_KEY,
            "client_email": (
                "test-only-email@test-only-project-id.iam.gserviceaccount.com"
            ),
            "client_id": "100000000000000000001",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": token_uri,
            "auth_provider_x509_cert_url": (
                "https://www.googleapis.com/oauth2/v1/certs"
            ),
            "client_x509_cert_url": (
                "https://www.googleapis.com/robot/v1/metadata/x509/"
                "foo-email%40foo-project.iam.gserviceaccount.com"
            ),
        }
    )


def _parse_rfc3339(value: str) -> datetime:
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    return datetime.fromisoformat(value)


def _make_ingestion_handler(
    capture_path: str, token_capture_path: str
) -> type[BaseHTTPRequestHandler]:
    lock = threading.Lock()

    class IngestionHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_POST(self) -> None:
            if self.path == "/token":
                self._handle_token()
                return
            auth = self.headers.get("Authorization", "")
            expected_token = (
                _TOKENS_BY_SCOPE[_INGESTION_SCOPE]
                if self.path == _INGESTION_PATH
                else _TOKENS_BY_SCOPE[_CLOUD_PLATFORM_SCOPE]
            )
            if auth != f"Bearer {expected_token}":
                self._json_response(401, {"error": "unauthorized"})
                return
            content_length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(content_length)
            encoding = self.headers.get("Content-Encoding", "")
            if encoding == "gzip":
                try:
                    raw = gzip.decompress(raw)
                except Exception as e:
                    self._json_response(400, {"error": f"gzip error: {e}"})
                    return
            try:
                payload = json.loads(raw)
            except Exception as e:
                self._json_response(400, {"error": f"JSON parse error: {e}"})
                return
            err = _validate_payload(self.path, payload)
            if err:
                self._json_response(400, {"error": err})
                return
            capture = {
                "path": self.path,
                "payload": payload,
            }
            with lock:
                with open(capture_path, "a") as f:
                    f.write(json.dumps(capture, sort_keys=True) + "\n")
            self._json_response(200, {})

        def _handle_token(self) -> None:
            content_length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(content_length).decode()
            fields = parse_qs(raw)
            grant_type = fields.get("grant_type", [""])[0]
            assertion = fields.get("assertion", [""])[0]
            if grant_type != "urn:ietf:params:oauth:grant-type:jwt-bearer":
                self._json_response(400, {"error": "unexpected grant_type"})
                return
            if len(assertion.split(".")) != 3:
                self._json_response(400, {"error": "invalid assertion"})
                return
            try:
                encoded_claims = assertion.split(".")[1]
                padding = "=" * (-len(encoded_claims) % 4)
                claims = json.loads(
                    base64.urlsafe_b64decode(encoded_claims + padding).decode()
                )
            except Exception as e:
                self._json_response(400, {"error": f"invalid claims: {e}"})
                return
            capture = {
                "grant_type": grant_type,
                "assertion_segments": len(assertion.split(".")),
                "issuer": claims.get("iss"),
                "scope": claims.get("scope"),
            }
            access_token = _TOKENS_BY_SCOPE.get(capture["scope"])
            if access_token is None:
                self._json_response(400, {"error": "unexpected scope"})
                return
            with lock:
                with open(token_capture_path, "a") as f:
                    f.write(json.dumps(capture, sort_keys=True) + "\n")
            self._json_response(
                200,
                {
                    "token_type": "Bearer",
                    "access_token": access_token,
                    "expires_in": 3600,
                },
            )

        def _json_response(self, code: int, obj: dict) -> None:  # type: ignore[type-arg]
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return IngestionHandler


def _validate_payload(path: str, payload: object) -> str | None:
    if not isinstance(payload, dict):
        return "payload must be a JSON object"
    if path == _INGESTION_PATH:
        return _validate_ingestion(payload)
    inline_source = payload.get("inlineSource")
    if not isinstance(inline_source, dict):
        return "missing inlineSource object"
    if _LOGS_PATH.match(path):
        hint = payload.get("hint")
        if hint is not None and not isinstance(hint, str):
            return "hint must be a string"
        return _validate_logs(inline_source)
    if _EVENTS_PATH.match(path):
        return _validate_events(inline_source)
    if _ENTITIES_PATH.match(path):
        return _validate_entities(inline_source)
    return f"unexpected path: {path}"


def _validate_ingestion(payload: dict) -> str | None:  # type: ignore[type-arg]
    for field in ("customer_id", "log_type", "namespace"):
        if not isinstance(payload.get(field), str) or not payload[field]:
            return f"{field} must be a non-empty string"
    labels = payload.get("labels", [])
    if not isinstance(labels, list):
        return "labels must be an array"
    for i, label in enumerate(labels):
        if not isinstance(label, dict):
            return f"labels[{i}] must be an object"
        if not isinstance(label.get("key"), str):
            return f"labels[{i}].key must be a string"
        if not isinstance(label.get("value"), str):
            return f"labels[{i}].value must be a string"
    entries = payload.get("entries")
    if not isinstance(entries, list) or len(entries) == 0:
        return "entries must be a non-empty array"
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            return f"entries[{i}] must be an object"
        if not isinstance(entry.get("log_text"), str):
            return f"entries[{i}].log_text must be a string"
        timestamp = entry.get("ts_epoch_microseconds")
        if timestamp is not None and (
            not isinstance(timestamp, int) or isinstance(timestamp, bool)
        ):
            return f"entries[{i}].ts_epoch_microseconds must be an integer"
    return None


def _validate_logs(inline_source: dict) -> str | None:  # type: ignore[type-arg]
    logs = inline_source.get("logs")
    if not isinstance(logs, list) or len(logs) == 0:
        return "inlineSource.logs must be a non-empty array"
    forwarder = inline_source.get("forwarder")
    if forwarder is not None and not isinstance(forwarder, str):
        return "inlineSource.forwarder must be a string"
    source_filename = inline_source.get("sourceFilename")
    if source_filename is not None and not isinstance(source_filename, str):
        return "inlineSource.sourceFilename must be a string"
    for i, log in enumerate(logs):
        if not isinstance(log, dict):
            return f"logs[{i}] must be an object"
        data = log.get("data")
        if not isinstance(data, str):
            return f"logs[{i}].data must be a string"
        try:
            base64.b64decode(data, validate=True)
        except Exception as e:
            return f"logs[{i}].data must be base64: {e}"
        log_entry_time = log.get("logEntryTime")
        collection_time = log.get("collectionTime")
        if not isinstance(log_entry_time, str):
            return f"logs[{i}].logEntryTime must be a string"
        if not isinstance(collection_time, str):
            return f"logs[{i}].collectionTime must be a string"
        try:
            if _parse_rfc3339(collection_time) <= _parse_rfc3339(log_entry_time):
                return f"logs[{i}].collectionTime must be after logEntryTime"
        except Exception as e:
            return f"logs[{i}] has invalid timestamp: {e}"
        namespace = log.get("environmentNamespace")
        if namespace is not None and not isinstance(namespace, str):
            return f"logs[{i}].environmentNamespace must be a string"
        labels = log.get("labels", {})
        if not isinstance(labels, dict):
            return f"logs[{i}].labels must be an object"
        for key, label in labels.items():
            if not isinstance(key, str) or not isinstance(label, dict):
                return f"logs[{i}].labels entries must be objects"
            if not isinstance(label.get("value"), str):
                return f"logs[{i}].labels.{key}.value must be a string"
            if not isinstance(label.get("rbacEnabled"), bool):
                return f"logs[{i}].labels.{key}.rbacEnabled must be a boolean"
    return None


def _validate_events(inline_source: dict) -> str | None:  # type: ignore[type-arg]
    events = inline_source.get("events")
    if not isinstance(events, list) or len(events) == 0:
        return "inlineSource.events must be a non-empty array"
    for i, event in enumerate(events):
        if not isinstance(event, dict):
            return f"events[{i}] must be an object"
        if not isinstance(event.get("udm"), dict):
            return f"events[{i}].udm must be an object"
    return None


def _validate_entities(inline_source: dict) -> str | None:  # type: ignore[type-arg]
    entities = inline_source.get("entities")
    if not isinstance(entities, list) or len(entities) == 0:
        return "inlineSource.entities must be a non-empty array"
    log_type = inline_source.get("logType")
    if not isinstance(log_type, str) or not log_type:
        return "inlineSource.logType must be a non-empty string"
    for i, entity in enumerate(entities):
        if not isinstance(entity, dict):
            return f"entities[{i}] must be an object"
        if not isinstance(entity.get("entity"), dict):
            return f"entities[{i}].entity must be an object"
    return None


@fixture()
def google_secops() -> Iterator[dict[str, str]]:
    fd, capture_path = tempfile.mkstemp(prefix="secops-capture-", suffix=".jsonl")
    os.close(fd)
    fd, token_capture_path = tempfile.mkstemp(
        prefix="secops-token-capture-", suffix=".jsonl"
    )
    os.close(fd)
    server = None
    thread = None
    try:
        handler = _make_ingestion_handler(capture_path, token_capture_path)
        server = HTTPServer((_HOST, 0), handler)
        port = server.server_port
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        token_uri = f"http://{_HOST}:{port}/token"
        env = {
            "GOOGLE_SECOPS_INGESTION_URL": f"http://{_HOST}:{port}",
            "GOOGLE_SECOPS_TOKEN_URL": token_uri,
            "GOOGLE_SECOPS_CAPTURE_FILE": capture_path,
            "GOOGLE_SECOPS_TOKEN_CAPTURE_FILE": token_capture_path,
            "GOOGLE_SECOPS_SERVICE_CREDENTIALS": _service_credentials(token_uri),
            "GOOGLE_SECOPS_PRIVATE_KEY": _PRIVATE_KEY,
            "GOOGLE_SECOPS_CLIENT_EMAIL": (
                "test-only-email@test-only-project-id.iam.gserviceaccount.com"
            ),
            "GOOGLE_SECOPS_CUSTOMER_ID": "1234567890",
            "GOOGLE_CLOUD_CPP_EXPERIMENTAL_DISABLE_SELF_SIGNED_JWT": "1",
        }
        yield env
    finally:
        if server is not None:
            server.shutdown()
        if thread is not None:
            thread.join(timeout=2)
        if os.path.exists(capture_path):
            os.remove(capture_path)
        if os.path.exists(token_capture_path):
            os.remove(token_capture_path)
