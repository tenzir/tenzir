"""Confluent-compatible schema registry with deterministic schemas and failures."""

from __future__ import annotations

import json
import tempfile
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tenzir_test import fixture

GUID = "00112233-4455-6677-8899-aabbccddeeff"
AUTHORIZATION = "Basic dGVzdDpzZWNyZXQ="
SCHEMA = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "optional", "type": ["null", "string"]},
    ],
}
SCHEMAS = {
    "1": {"schema": json.dumps(SCHEMA)},
    "2": {
        "schema": json.dumps(
            {
                **SCHEMA,
                "fields": [*SCHEMA["fields"], {"name": "active", "type": "boolean"}],
            }
        ),
    },
    "3": {"schema": '"bytes"'},
    "4": {"schema": '"null"'},
    "5": {
        "schema": json.dumps(
            {
                "type": "record",
                "name": "Parent",
                "fields": [{"name": "child", "type": "Child"}],
            }
        ),
        "references": [{"name": "Child", "subject": "child/schema", "version": 1}],
    },
    "6": {"schemaType": "JSON", "schema": "{}"},
    "7": {"schema": '"long"'},
    "8": {"schema": "not valid JSON"},
    "9": {"schema": '"long"'},
}

# Eleven distinct schemas, with overlapping dependencies that would cause more
# than 128 lookups without memoization. Null unions keep the datum small.
SHARED_SCHEMAS = {}
for index in range(11):
    children = [child for child in (index + 1, index + 2) if child < 11]
    SHARED_SCHEMAS[index] = {
        "schema": json.dumps(
            {
                "type": "record",
                "name": f"Node{index}",
                "fields": [
                    {"name": f"child{child}", "type": ["null", f"Node{child}"]}
                    for child in children
                ],
            }
        ),
        "references": [
            {"name": f"Node{child}", "subject": f"node{child}", "version": 1}
            for child in children
        ],
    }
SCHEMAS["10"] = SHARED_SCHEMAS[0]


@fixture
def avro_registry() -> Iterator[dict[str, str]]:
    with tempfile.TemporaryDirectory(prefix="tenzir-avro-registry-") as directory:
        capture = Path(directory) / "requests.jsonl"
        capture.touch()
        attempts: dict[str, int] = {}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_: object) -> None:
                pass

            def do_GET(self) -> None:
                with capture.open("a") as file:
                    file.write(
                        json.dumps(
                            {
                                "path": self.path,
                                "authorization": self.headers.get("Authorization"),
                            }
                        )
                        + "\n"
                    )
                attempts[self.path] = attempts.get(self.path, 0) + 1
                status = 200
                result: object = {"error_code": 40403, "message": "Schema not found"}
                prefix, _, path = self.path[1:].partition("/")
                if prefix == "slow" and path == "schemas/ids/1":
                    # Deliberately exceed the consumer's six-second poll limit.
                    time.sleep(8)
                if (
                    self.headers.get("Authorization") != AUTHORIZATION
                    or prefix == "unauthorized"
                ):
                    status = 401
                elif prefix == "missing":
                    status = 404
                elif prefix == "retry" and attempts[self.path] == 1:
                    status = 503
                elif prefix == "oversized":
                    # Leave the response incomplete. A bounded client must
                    # disconnect without waiting for the remaining body.
                    self.send_response(200)
                    self.send_header("Content-Length", str(64 * 1024 * 1024))
                    self.end_headers()
                    self.connection.settimeout(5)
                    try:
                        for _ in range(17):
                            self.wfile.write(b" " * (1024 * 1024))
                        disconnected = self.connection.recv(1) == b""
                    except (BrokenPipeError, ConnectionResetError):
                        disconnected = True
                    except TimeoutError:
                        disconnected = False
                    with capture.open("a") as file:
                        file.write(json.dumps({"disconnected": disconnected}) + "\n")
                    return
                elif path == f"schemas/guids/{GUID}":
                    result = SCHEMAS["1"]
                elif path.startswith("schemas/ids/"):
                    identifier = path.removeprefix("schemas/ids/")
                    result = SCHEMAS.get(identifier, result)
                    if identifier not in SCHEMAS:
                        status = 404
                    elif prefix == "wrong-schema" and identifier == "7":
                        result = {
                            "schema": json.dumps(
                                {
                                    "type": "record",
                                    "name": "Wrong",
                                    "fields": [
                                        {"name": "a", "type": "long"},
                                        {"name": "b", "type": "long"},
                                    ],
                                }
                            )
                        }
                elif path == "subjects/child%2Fschema/versions/1?deleted=true":
                    result = {
                        "deleted": True,
                        "schema": json.dumps(
                            {
                                "type": "record",
                                "name": "Child",
                                "fields": [{"name": "n", "type": "long"}],
                            }
                        ),
                    }
                elif path.startswith("subjects/node") and path.endswith(
                    "/versions/1?deleted=true"
                ):
                    index = int(path.split("/")[1].removeprefix("node"))
                    result = SHARED_SCHEMAS[index]
                else:
                    status = 404
                body = json.dumps(result).encode()
                self.send_response(status)
                self.send_header(
                    "Content-Type", "application/vnd.schemaregistry.v1+json"
                )
                self.send_header("Content-Length", str(len(body)))
                if status == 503:
                    self.send_header("Retry-After", "0")
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield {
                "AVRO_REGISTRY_URL": f"http://127.0.0.1:{server.server_port}",
                "AVRO_REGISTRY_CAPTURE": str(capture),
            }
        finally:
            server.shutdown()
            thread.join()
            server.server_close()
