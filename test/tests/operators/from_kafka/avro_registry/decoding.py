# runner: python

from __future__ import annotations

import base64
import json
import os
import subprocess
import urllib.request
from pathlib import Path

binary = os.environ["TENZIR_PYTHON_FIXTURE_BINARY"]
registry = os.environ["AVRO_REGISTRY_URL"]
control = "http://" + os.environ["KAFKA_IAM_HELPER_HTTP"]
header_key = "__value_schema_id"
guid = bytes.fromhex("00112233445566778899aabbccddeeff")


def b64(value: bytes | None) -> str | None:
    return base64.b64encode(value).decode() if value is not None else None


def record(value: bytes | None, headers: list[tuple[str, bytes | None]] = ()) -> dict:
    return {
        "value": b64(value),
        "headers": [{"key": key, "value": b64(data)} for key, data in headers],
    }


def prefix(identifier: int, value: bytes) -> bytes:
    return b"\x00" + identifier.to_bytes(4, "big") + value


def seed(topic: str, records: list[dict]) -> None:
    request = urllib.request.Request(
        control + "/seed",
        json.dumps({"topic": topic, "records": records}).encode(),
        {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        assert response.status == 204


def consume(
    topic: str,
    count: int,
    mode: str = "registry",
    group: str | None = None,
    stored: bool = False,
    max_poll_interval: int = 300_000,
) -> subprocess.CompletedProcess[str]:
    q = json.dumps
    pipeline = f"""from_kafka {q(topic)},
      count={count}, offset={q("stored" if stored else "beginning")},
      schema_registry={q(registry + "/" + mode + "/")},
      schema_registry_headers={{Authorization: "Basic dGVzdDpzZWNyZXQ="}},
      aws_region={q(os.environ["KAFKA_AWS_REGION"])},
      aws_iam={{
        access_key_id: {q(os.environ["KAFKA_AWS_ACCESS_KEY_ID"])},
        secret_access_key: {q(os.environ["KAFKA_AWS_SECRET_ACCESS_KEY"])}
      }},
      _optimization="unordered", _worker_batch_size=1,
      options={{
        "bootstrap.servers": {q(os.environ["KAFKA_BOOTSTRAP_SERVERS"])},
        "group.id": {q(group or topic)},
        "auto.offset.reset": "earliest",
        "security.protocol": "SASL_PLAINTEXT",
        "enable.metrics.push": false,
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 1000,
        "max.poll.interval.ms": {max_poll_interval}
      }}
    write_ndjson
    """
    return subprocess.run(
        [binary, pipeline], capture_output=True, text=True, timeout=35, check=False
    )


def rows(result: subprocess.CompletedProcess[str]) -> list[dict]:
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]


# Same schema via both identifiers, schema evolution, last-header precedence,
# null-header fallback, and tombstones. Header payload starts with 0x00.
data = b"\x00\x02a\x00"  # {id: 0, name: "a", optional: null}
messages = [
    record(prefix(1, data)),
    record(b"\x01" + guid + data),
    record(data, [(header_key, b"\x01" + guid)]),
    record(prefix(2, data + b"\x01")),
    record(data, [(header_key, b"bad"), (header_key, b"\x01" + guid)]),
    record(prefix(1, data), [(header_key, None)]),
    record(None),
]
seed("avro_mixed", messages)
expected = {"id": 0, "name": "a", "optional": None}
assert rows(consume("avro_mixed", len(messages))) == [
    expected,
    expected,
    expected,
    {**expected, "active": True},
    expected,
    expected,
]
requests = [
    json.loads(line)["path"]
    for line in Path(os.environ["AVRO_REGISTRY_CAPTURE"]).read_text().splitlines()
]
assert requests.count("/registry/schemas/ids/1") == 1, requests
assert (
    requests.count("/registry/schemas/guids/00112233-4455-6677-8899-aabbccddeeff") == 1
), requests
print("mixed framing, schema evolution, header precedence, tombstones, and cache: ok")

seed(
    "avro_values",
    [
        record(prefix(3, b"abc")),
        record(prefix(4, b"")),
        record(prefix(5, b"\x0e")),
        record(b"", [(header_key, b"\x00\x00\x00\x00\x04")]),
    ],
)
values = rows(consume("avro_values", 4))
assert values == [
    {"value": "YWJj"},
    {"value": None},
    {"child": {"n": 7}},
    {"value": None},
], values
print("top-level bytes, null, empty payload, and schema references: ok")

cases = [
    ("magic", record(b"\x02garbage"), "unsupported Confluent"),
    ("short_id", record(b"\x00\x01"), "truncated Confluent schema ID"),
    (
        "short_guid",
        record(data, [(header_key, b"\x01")]),
        "truncated Confluent schema GUID",
    ),
    (
        "header_precedence",
        record(prefix(1, data), [(header_key, b"bad")]),
        "unsupported Confluent",
    ),
    ("empty_header", record(prefix(1, data), [(header_key, b"")]), "missing Confluent"),
    ("truncated", record(prefix(1, b"\x00")), "truncated Avro datum"),
    ("trailing", record(prefix(1, data + b"\x00")), "unexpected trailing bytes"),
    ("wrong_type", record(prefix(6, data)), "non-Avro schema"),
    ("invalid_schema", record(prefix(8, data)), "failed to resolve Avro schema"),
]
for name, message, error in cases:
    topic = "avro_error_" + name
    seed(topic, [message])
    result = consume(topic, 1)
    assert result.returncode != 0 and error in result.stderr, (
        name,
        result.stdout,
        result.stderr,
    )
print("invalid framing, payloads, and schemas: ok")

# Even when unordered optimization is requested, a failed lookup or decode
# must not commit this or any later batch from the concurrent workers.
for mode, error in [
    ("missing", "HTTP 404"),
    ("unauthorized", "HTTP 401"),
    ("wrong-schema", "truncated Avro datum"),
]:
    topic = "avro_offsets_" + mode
    seed(topic, [record(prefix(7, b"\x02")), record(prefix(9, b"\x04"))])
    result = consume(topic, 2, mode, group=topic)
    assert result.returncode != 0 and error in result.stderr, result.stderr
    assert rows(consume(topic, 2, group=topic, stored=True)) == [
        {"value": 1},
        {"value": 2},
    ]
# Successful preceding batches may commit, but a later valid batch must not
# advance the offset past a failure in another worker.
seed(
    "avro_partial",
    [
        record(prefix(9, b"\x00")),
        record(prefix(7, b"\x02")),
        record(prefix(9, b"\x04")),
    ],
)
result = consume("avro_partial", 3, "wrong-schema")
assert result.returncode != 0 and "truncated Avro datum" in result.stderr, result.stderr
assert rows(consume("avro_partial", 2, stored=True)) == [{"value": 1}, {"value": 2}]
print("registry and decode failures preserve offsets across restart: ok")

seed("avro_retry", [record(prefix(7, b"\x02"))])
assert rows(consume("avro_retry", 1, "retry")) == [{"value": 1}]
requests = [
    json.loads(line)["path"]
    for line in Path(os.environ["AVRO_REGISTRY_CAPTURE"]).read_text().splitlines()
]
assert requests.count("/retry/schemas/ids/7") == 2, requests
print("transient registry failure retry: ok")

seed("avro_shared", [record(prefix(10, b"\x00\x00"))])
assert rows(consume("avro_shared", 1)) == [{"child1": None, "child2": None}]
requests = [
    json.loads(line)["path"]
    for line in Path(os.environ["AVRO_REGISTRY_CAPTURE"]).read_text().splitlines()
]
for index in range(1, 11):
    path = f"/registry/subjects/node{index}/versions/1?deleted=true"
    assert requests.count(path) == 1
assert requests.count("/registry/subjects/child%2Fschema/versions/1?deleted=true") == 1
print("shared dependencies fetched once and soft-deleted references resolved: ok")

seed("avro_oversized", [record(prefix(1, data))])
result = consume("avro_oversized", 1, "oversized")
assert result.returncode != 0 and "exceeds 16 MiB" in result.stderr, result.stderr
captures = [
    json.loads(line)
    for line in Path(os.environ["AVRO_REGISTRY_CAPTURE"]).read_text().splitlines()
]
assert {"disconnected": True} in captures, captures
print("oversized registry response stopped before EOF: ok")

seed(
    "avro_slow",
    [record(prefix(1, bytes([2 * index]) + data[1:])) for index in range(4)],
)
result = consume("avro_slow", 3, "slow", max_poll_interval=6000)
assert rows(result) == [{**expected, "id": index} for index in range(3)]
assert "MAXPOLL" not in result.stderr and "max.poll.interval.ms" not in result.stderr
assert rows(consume("avro_slow", 1, stored=True)) == [{**expected, "id": 3}]
print("slow registry preserves group membership and stored offsets: ok")
