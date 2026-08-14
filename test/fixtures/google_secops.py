"""Mock ingestion server for to_google_secops integration tests."""

# /// script
# dependencies = ["cryptography"]
# ///

from __future__ import annotations

import base64
import gzip
import json
import os
import re
import tempfile
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from typing import Iterator
from urllib.parse import parse_qs

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except ImportError:
    hashes = None  # type: ignore[assignment]
    serialization = None  # type: ignore[assignment]
    padding = None  # type: ignore[assignment]

_HOST = "127.0.0.1"
_CLIENT_EMAIL = "test-only-email@test-only-project-id.iam.gserviceaccount.com"
_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_INGESTION_SCOPE = "https://www.googleapis.com/auth/malachite-ingestion"
_INGESTION_TOKEN_AUDIENCE = "https://oauth2.googleapis.com/token"
_MAX_ASSERTION_LIFETIME_SECONDS = 3600
_CLOCK_SKEW_SECONDS = 60
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
_PUBLIC_KEY = (
    serialization.load_pem_private_key(
        _PRIVATE_KEY.encode(), password=None
    ).public_key()
    if serialization is not None
    else None
)
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
            "client_email": _CLIENT_EMAIL,
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


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _parse_rfc3339(value: str) -> datetime:
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    return datetime.fromisoformat(value)


def _validate_jwt_claims(
    claims: dict, expected_audiences: dict[str, str]
) -> str | None:  # type: ignore[type-arg]
    if claims.get("iss") != _CLIENT_EMAIL:
        return "unexpected issuer"
    scope = claims.get("scope")
    if not isinstance(scope, str) or scope not in _TOKENS_BY_SCOPE:
        return "unexpected scope"
    if claims.get("aud") != expected_audiences[scope]:
        return "unexpected audience"
    issued_at = claims.get("iat")
    expires_at = claims.get("exp")
    if not isinstance(issued_at, int) or isinstance(issued_at, bool):
        return "iat must be an integer"
    if not isinstance(expires_at, int) or isinstance(expires_at, bool):
        return "exp must be an integer"
    lifetime = expires_at - issued_at
    if lifetime <= 0 or lifetime > _MAX_ASSERTION_LIFETIME_SECONDS:
        return "invalid assertion lifetime"
    now = int(time.time())
    if issued_at > now + _CLOCK_SKEW_SECONDS:
        return "assertion issued in the future"
    if expires_at <= now - _CLOCK_SKEW_SECONDS:
        return "assertion expired"
    return None


def _make_ingestion_handler(
    capture_path: str, token_capture_path: str
) -> type[BaseHTTPRequestHandler]:
    lock = threading.Lock()

    class IngestionHandler(BaseHTTPRequestHandler):
        token_audiences: dict[str, str] = {}

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
            segments = assertion.split(".")
            if len(segments) != 3:
                self._json_response(400, {"error": "invalid assertion"})
                return
            encoded_header, encoded_claims, encoded_signature = segments
            try:
                header = json.loads(_b64url_decode(encoded_header).decode())
                claims = json.loads(_b64url_decode(encoded_claims).decode())
                signature = _b64url_decode(encoded_signature)
            except Exception as e:
                self._json_response(400, {"error": f"invalid assertion: {e}"})
                return
            if not isinstance(header, dict) or not isinstance(claims, dict):
                self._json_response(400, {"error": "invalid assertion objects"})
                return
            if header.get("alg") != "RS256":
                self._json_response(
                    400, {"error": f"unexpected alg: {header.get('alg')}"}
                )
                return
            try:
                _PUBLIC_KEY.verify(
                    signature,
                    f"{encoded_header}.{encoded_claims}".encode(),
                    padding.PKCS1v15(),
                    hashes.SHA256(),
                )
            except Exception:
                self._json_response(400, {"error": "invalid signature"})
                return
            if err := _validate_jwt_claims(claims, self.token_audiences):
                self._json_response(400, {"error": err})
                return
            capture = {
                "grant_type": grant_type,
                "assertion_segments": len(segments),
                "issuer": claims.get("iss"),
                "scope": claims.get("scope"),
                "audience": claims.get("aud"),
                "issued_at": claims.get("iat"),
                "expires_at": claims.get("exp"),
                "claims_validated": True,
                "signature_verified": True,
            }
            access_token = _TOKENS_BY_SCOPE[capture["scope"]]
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
    if _PUBLIC_KEY is None:
        raise FixtureUnavailable(
            "cryptography not installed; install with: pip install cryptography"
        )
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
        token_uri = f"http://{_HOST}:{port}/token"
        handler.token_audiences = {
            _CLOUD_PLATFORM_SCOPE: token_uri,
            _INGESTION_SCOPE: _INGESTION_TOKEN_AUDIENCE,
        }
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        env = {
            "GOOGLE_SECOPS_INGESTION_URL": f"http://{_HOST}:{port}",
            "GOOGLE_SECOPS_TOKEN_URL": token_uri,
            "GOOGLE_SECOPS_CAPTURE_FILE": capture_path,
            "GOOGLE_SECOPS_TOKEN_CAPTURE_FILE": token_capture_path,
            "GOOGLE_SECOPS_SERVICE_CREDENTIALS": _service_credentials(token_uri),
            "GOOGLE_SECOPS_PRIVATE_KEY": _PRIVATE_KEY,
            "GOOGLE_SECOPS_CLIENT_EMAIL": _CLIENT_EMAIL,
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


def _make_backpressure_handler(
    state_path: str, release_path: str
) -> type[BaseHTTPRequestHandler]:
    condition = threading.Condition()
    initial_group_sizes = {
        "FAIRNESS": 2,
        "LATE_SUCCESS": 3,
        "OVERLAP": 2,
        "PRESERVE": 2,
        "PROBE400": 3,
        "RETRY": 2,
        "RETRY429": 2,
    }
    state: dict[str, object] = {
        "arrivals": 0,
        "blocked": 0,
        "max_blocked": 0,
        "arrival_order": [],
        "attempts": {},
        "retry_inflight": {},
        "max_retry_inflight": {},
        "initial_arrivals": {},
        "fairness_retry_order": [],
        "fairness_stuck_probe_started": False,
        "fairness_other_initial_sent": False,
        "progressive_arrivals": 0,
        "progressive_order": [],
        "event_bound_batches": [],
        "preserve_late_at": None,
        "preserve_probe_third_at": None,
        "preserve_probe_second_inflight": False,
        "preserve_late_sent": False,
        "probe400_late_initials": 0,
        "probe400_probe_second_inflight": False,
        "overlap_probe_second_inflight": False,
        "overlap_late_sent": False,
        "late_success_probe_second_inflight": False,
        "late_success_queued_initial_sent": False,
        "late_success_queued_retry_started": False,
        "late_success_unblocked_before_probe": False,
    }

    def write_state() -> None:
        temporary_path = f"{state_path}.tmp"
        with open(temporary_path, "w") as file:
            json.dump(state, file)
        os.replace(temporary_path, state_path)

    class BackpressureHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(content_length)
            if self.path == "/token":
                self._json_response(
                    200,
                    {
                        "token_type": "Bearer",
                        "access_token": _TOKENS_BY_SCOPE[_CLOUD_PLATFORM_SCOPE],
                        "expires_in": 3600,
                    },
                )
                return
            if self.headers.get("Content-Encoding", "") == "gzip":
                raw = gzip.decompress(raw)
            payload = json.loads(raw)
            log_type = self.path.split("/logTypes/", 1)[1].split("/", 1)[0]
            if log_type.startswith("BACKPRESSURE_"):
                with condition:
                    state["arrivals"] = int(state["arrivals"]) + 1
                    state["blocked"] = int(state["blocked"]) + 1
                    arrival_order = state["arrival_order"]
                    assert isinstance(arrival_order, list)
                    arrival_order.append(log_type)
                    state["max_blocked"] = max(
                        int(state["max_blocked"]), int(state["blocked"])
                    )
                    write_state()
                while not os.path.exists(release_path):
                    time.sleep(0.01)
                with condition:
                    state["blocked"] = int(state["blocked"]) - 1
                    write_state()
                self._json_response(200, {})
                return
            if log_type.startswith("PROGRESS_"):
                with condition:
                    state["progressive_arrivals"] = (
                        int(state["progressive_arrivals"]) + 1
                    )
                    progressive_order = state["progressive_order"]
                    assert isinstance(progressive_order, list)
                    progressive_order.append(log_type)
                    write_state()
                while not os.path.exists(release_path):
                    time.sleep(0.01)
                self._json_response(200, {})
                return
            if log_type == "EVENT_BOUND":
                logs = payload["inlineSource"]["logs"]
                with condition:
                    event_bound_batches = state["event_bound_batches"]
                    assert isinstance(event_bound_batches, list)
                    event_bound_batches.append(len(logs))
                    write_state()
                while not os.path.exists(release_path):
                    time.sleep(0.01)
                self._json_response(200, {})
                return
            if log_type in {
                "FAIRNESS_OTHER",
                "FAIRNESS_STUCK",
                "OVERLAP_LATE",
                "OVERLAP_PROBE",
                "LATE_SUCCESS_LATE",
                "LATE_SUCCESS_PROBE",
                "LATE_SUCCESS_QUEUED",
                "PERMANENT",
                "PRESERVE_LATE",
                "PRESERVE_PROBE",
                "PROBE400_A",
                "PROBE400_B",
                "PROBE400_PROBE",
                "RETRY_A",
                "RETRY_B",
                "RETRY_LONG",
                "RETRY429_A",
                "RETRY429_B",
            }:
                if log_type.startswith("FAIRNESS_"):
                    group = "FAIRNESS"
                elif log_type.startswith("RETRY429_"):
                    group = "RETRY429"
                elif log_type.startswith("LATE_SUCCESS_"):
                    group = "LATE_SUCCESS"
                elif log_type.startswith("OVERLAP_"):
                    group = "OVERLAP"
                elif log_type.startswith("PRESERVE_"):
                    group = "PRESERVE"
                elif log_type.startswith("PROBE400_"):
                    group = "PROBE400"
                elif log_type in {"RETRY_A", "RETRY_B"}:
                    group = "RETRY"
                else:
                    group = log_type
                with condition:
                    attempts = state["attempts"]
                    assert isinstance(attempts, dict)
                    attempt = int(attempts.get(log_type, 0)) + 1
                    attempts[log_type] = attempt
                    if attempt == 1 and group in initial_group_sizes:
                        initial_arrivals = state["initial_arrivals"]
                        assert isinstance(initial_arrivals, dict)
                        initial_arrivals[group] = (
                            int(initial_arrivals.get(group, 0)) + 1
                        )
                        condition.notify_all()
                    track_inflight = group in {
                        "OVERLAP",
                        "PROBE400",
                        "RETRY",
                        "RETRY429",
                    }
                    if attempt > 1 and track_inflight:
                        retry_inflight = state["retry_inflight"]
                        max_retry_inflight = state["max_retry_inflight"]
                        assert isinstance(retry_inflight, dict)
                        assert isinstance(max_retry_inflight, dict)
                        retry_inflight[group] = int(retry_inflight.get(group, 0)) + 1
                        max_retry_inflight[group] = max(
                            int(max_retry_inflight.get(group, 0)),
                            retry_inflight[group],
                        )
                    write_state()

                def finish_retry_attempt() -> None:
                    if attempt <= 1 or not track_inflight:
                        return
                    with condition:
                        retry_inflight = state["retry_inflight"]
                        assert isinstance(retry_inflight, dict)
                        retry_inflight[group] = int(retry_inflight[group]) - 1
                        write_state()

                def wait_for_initial_group() -> None:
                    if attempt != 1 or group not in initial_group_sizes:
                        return
                    with condition:
                        initial_arrivals = state["initial_arrivals"]
                        assert isinstance(initial_arrivals, dict)
                        assert condition.wait_for(
                            lambda: (
                                int(initial_arrivals.get(group, 0))
                                == initial_group_sizes[group]
                            ),
                            timeout=2,
                        )

                if group == "FAIRNESS":
                    if attempt == 1:
                        wait_for_initial_group()
                        if log_type == "FAIRNESS_OTHER":
                            # Hold the second initial failure until the first
                            # request is the active probe. This makes the slot
                            # that should be rotated away from deterministic.
                            with condition:
                                assert condition.wait_for(
                                    lambda: bool(state["fairness_stuck_probe_started"]),
                                    timeout=2,
                                )
                        self._json_response(
                            500, {"error": "retry"}, {"Retry-After": "0"}
                        )
                        if log_type == "FAIRNESS_OTHER":
                            with condition:
                                state["fairness_other_initial_sent"] = True
                                write_state()
                                condition.notify_all()
                    else:
                        with condition:
                            retry_order = state["fairness_retry_order"]
                            assert isinstance(retry_order, list)
                            retry_order.append(log_type)
                            if log_type == "FAIRNESS_STUCK" and attempt == 2:
                                state["fairness_stuck_probe_started"] = True
                                write_state()
                                condition.notify_all()
                                assert condition.wait_for(
                                    lambda: bool(state["fairness_other_initial_sent"]),
                                    timeout=2,
                                )
                            else:
                                write_state()
                        if log_type == "FAIRNESS_STUCK" and attempt == 2:
                            # Give the coordinator time to queue the other
                            # failure before this probe closes the cooldown.
                            time.sleep(0.2)
                            self._json_response(
                                500, {"error": "retry"}, {"Retry-After": "0"}
                            )
                        else:
                            self._json_response(200, {})
                elif group == "PRESERVE":
                    if log_type == "PRESERVE_PROBE" and attempt == 1:
                        wait_for_initial_group()
                        self._json_response(
                            500, {"error": "retry"}, {"Retry-After": "0"}
                        )
                    elif log_type == "PRESERVE_LATE" and attempt == 1:
                        with condition:
                            assert condition.wait_for(
                                lambda: bool(state["preserve_probe_second_inflight"]),
                                timeout=2,
                            )
                            state["preserve_late_at"] = time.monotonic()
                            write_state()
                        self._json_response(
                            429, {"error": "retry"}, {"Retry-After": "2"}
                        )
                        with condition:
                            state["preserve_late_sent"] = True
                            write_state()
                            condition.notify_all()
                    elif log_type == "PRESERVE_PROBE" and attempt == 2:
                        with condition:
                            state["preserve_probe_second_inflight"] = True
                            write_state()
                            condition.notify_all()
                            assert condition.wait_for(
                                lambda: bool(state["preserve_late_sent"]), timeout=2
                            )
                        time.sleep(0.2)
                        self._json_response(
                            500, {"error": "retry"}, {"Retry-After": "0"}
                        )
                    else:
                        if log_type == "PRESERVE_PROBE" and attempt == 3:
                            with condition:
                                state["preserve_probe_third_at"] = time.monotonic()
                                write_state()
                        self._json_response(200, {})
                elif group == "PROBE400":
                    if attempt == 1:
                        wait_for_initial_group()
                        if log_type != "PROBE400_PROBE":
                            with condition:
                                assert condition.wait_for(
                                    lambda: bool(
                                        state["probe400_probe_second_inflight"]
                                    ),
                                    timeout=2,
                                )
                        self._json_response(
                            500, {"error": "retry"}, {"Retry-After": "0"}
                        )
                        if log_type != "PROBE400_PROBE":
                            with condition:
                                state["probe400_late_initials"] = (
                                    int(state["probe400_late_initials"]) + 1
                                )
                                write_state()
                                condition.notify_all()
                    elif log_type == "PROBE400_PROBE":
                        with condition:
                            state["probe400_probe_second_inflight"] = True
                            write_state()
                            condition.notify_all()
                            assert condition.wait_for(
                                lambda: int(state["probe400_late_initials"]) == 2,
                                timeout=2,
                            )
                        time.sleep(0.2)
                        finish_retry_attempt()
                        self._json_response(400, {"error": "permanent"})
                    else:
                        time.sleep(0.2)
                        finish_retry_attempt()
                        self._json_response(200, {})
                elif group == "LATE_SUCCESS":
                    if attempt == 1:
                        wait_for_initial_group()
                        if log_type == "LATE_SUCCESS_PROBE":
                            self._json_response(
                                500, {"error": "retry"}, {"Retry-After": "0"}
                            )
                        elif log_type == "LATE_SUCCESS_QUEUED":
                            with condition:
                                assert condition.wait_for(
                                    lambda: bool(
                                        state["late_success_probe_second_inflight"]
                                    ),
                                    timeout=2,
                                )
                            self._json_response(
                                500, {"error": "retry"}, {"Retry-After": "0"}
                            )
                            with condition:
                                state["late_success_queued_initial_sent"] = True
                                write_state()
                                condition.notify_all()
                        else:
                            with condition:
                                assert condition.wait_for(
                                    lambda: (
                                        bool(
                                            state["late_success_probe_second_inflight"]
                                        )
                                        and bool(
                                            state["late_success_queued_initial_sent"]
                                        )
                                    ),
                                    timeout=2,
                                )
                            time.sleep(0.2)
                            self._json_response(200, {})
                    elif log_type == "LATE_SUCCESS_PROBE":
                        with condition:
                            state["late_success_probe_second_inflight"] = True
                            write_state()
                            condition.notify_all()
                            unblocked = condition.wait_for(
                                lambda: bool(
                                    state["late_success_queued_retry_started"]
                                ),
                                timeout=2,
                            )
                            state["late_success_unblocked_before_probe"] = unblocked
                            write_state()
                        self._json_response(200, {})
                    else:
                        with condition:
                            state["late_success_queued_retry_started"] = True
                            write_state()
                            condition.notify_all()
                        self._json_response(200, {})
                elif log_type == "PERMANENT":
                    self._json_response(400, {"error": "permanent"})
                elif log_type == "RETRY_LONG" and attempt <= 12:
                    self._json_response(500, {"error": "retry"}, {"Retry-After": "0"})
                elif group == "OVERLAP":
                    if attempt == 1:
                        wait_for_initial_group()
                        if log_type == "OVERLAP_LATE":
                            with condition:
                                assert condition.wait_for(
                                    lambda: bool(
                                        state["overlap_probe_second_inflight"]
                                    ),
                                    timeout=2,
                                )
                        self._json_response(
                            500, {"error": "retry"}, {"Retry-After": "0"}
                        )
                        if log_type == "OVERLAP_LATE":
                            with condition:
                                state["overlap_late_sent"] = True
                                write_state()
                                condition.notify_all()
                    elif log_type == "OVERLAP_PROBE":
                        with condition:
                            state["overlap_probe_second_inflight"] = True
                            write_state()
                            condition.notify_all()
                            assert condition.wait_for(
                                lambda: bool(state["overlap_late_sent"]), timeout=2
                            )
                        time.sleep(0.2)
                        finish_retry_attempt()
                        self._json_response(200, {})
                    else:
                        finish_retry_attempt()
                        self._json_response(200, {})
                elif attempt == 1:
                    wait_for_initial_group()
                    code = 429 if group == "RETRY429" else 500
                    headers = {"Retry-After": "1"} if code == 429 else {}
                    self._json_response(code, {"error": "retry"}, headers)
                else:
                    # Keep the probe active long enough to detect whether a
                    # second retry was admitted concurrently.
                    time.sleep(0.2)
                    finish_retry_attempt()
                    self._json_response(200, {})
                return
            err = _validate_payload(self.path, payload)
            if err:
                self._json_response(400, {"error": err})
                return
            self._json_response(200, {})

        def _json_response(
            self,
            code: int,
            obj: dict,  # type: ignore[type-arg]
            headers: dict[str, str] | None = None,
        ) -> None:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

    write_state()
    return BackpressureHandler


@fixture(name="google_secops_backpressure")
def google_secops_backpressure() -> Iterator[dict[str, str]]:
    state_fd, state_path = tempfile.mkstemp(
        prefix="secops-backpressure-", suffix=".json"
    )
    os.close(state_fd)
    release_path = f"{state_path}.release"
    server = None
    thread = None
    try:
        handler = _make_backpressure_handler(state_path, release_path)
        server = ThreadingHTTPServer((_HOST, 0), handler)
        port = server.server_port
        token_uri = f"http://{_HOST}:{port}/token"
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        yield {
            "GOOGLE_SECOPS_BP_URL": f"http://{_HOST}:{port}",
            "GOOGLE_SECOPS_BP_STATE_FILE": state_path,
            "GOOGLE_SECOPS_BP_RELEASE_FILE": release_path,
            "GOOGLE_SECOPS_BP_SERVICE_CREDENTIALS": _service_credentials(token_uri),
        }
    finally:
        if server is not None:
            server.shutdown()
        if thread is not None:
            thread.join(timeout=2)
        for path in (state_path, release_path):
            if os.path.exists(path):
                os.remove(path)
