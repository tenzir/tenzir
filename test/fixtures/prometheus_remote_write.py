"""Prometheus Remote Write receiver fixture."""

from __future__ import annotations

import json
import shutil
import struct
import tempfile
import threading
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, pos
        shift += 7


def _snappy_decompress(buf: bytes) -> bytes:
    expected_size, pos = _read_varint(buf, 0)
    out = bytearray()
    while pos < len(buf):
        tag = buf[pos]
        pos += 1
        tag_type = tag & 0x03
        if tag_type == 0:
            length = tag >> 2
            if length < 60:
                length += 1
            else:
                size_len = length - 59
                length = int.from_bytes(buf[pos : pos + size_len], "little") + 1
                pos += size_len
            out.extend(buf[pos : pos + length])
            pos += length
            continue
        if tag_type == 1:
            length = 4 + ((tag >> 2) & 0x07)
            offset = ((tag & 0xE0) << 3) | buf[pos]
            pos += 1
        elif tag_type == 2:
            length = 1 + (tag >> 2)
            offset = int.from_bytes(buf[pos : pos + 2], "little")
            pos += 2
        else:
            length = 1 + (tag >> 2)
            offset = int.from_bytes(buf[pos : pos + 4], "little")
            pos += 4
        for _ in range(length):
            out.append(out[-offset])
    if len(out) != expected_size:
        raise ValueError(
            f"snappy size mismatch: expected {expected_size}, got {len(out)}"
        )
    return bytes(out)


def _fields(buf: bytes):
    pos = 0
    while pos < len(buf):
        key, pos = _read_varint(buf, pos)
        number = key >> 3
        wire_type = key & 0x07
        if wire_type == 0:
            value, pos = _read_varint(buf, pos)
        elif wire_type == 1:
            value = buf[pos : pos + 8]
            pos += 8
        elif wire_type == 2:
            length, pos = _read_varint(buf, pos)
            value = buf[pos : pos + length]
            pos += length
        elif wire_type == 5:
            value = buf[pos : pos + 4]
            pos += 4
        else:
            raise ValueError(f"unsupported wire type {wire_type}")
        yield number, wire_type, value


def _parse_label(buf: bytes) -> tuple[str, str]:
    name = ""
    value = ""
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            name = field.decode()
        elif number == 2:
            value = field.decode()
    return name, value


def _parse_sample(buf: bytes) -> dict[str, object]:
    sample: dict[str, object] = {}
    for number, wire_type, field in _fields(buf):
        if number == 1 and wire_type == 1:
            sample["value"] = struct.unpack("<d", field)[0]
        elif number == 2:
            sample["timestamp"] = field
        elif number == 3:
            sample["start_timestamp"] = field
    return sample


def _parse_metadata_v1(buf: bytes) -> dict[str, object]:
    result: dict[str, object] = {}
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            result["type"] = field
        elif number == 2:
            result["family"] = field.decode()
        elif number == 4:
            result["help"] = field.decode()
        elif number == 5:
            result["unit"] = field.decode()
    return result


def _parse_v1(buf: bytes) -> dict[str, object]:
    result: dict[str, object] = {"timeseries": [], "metadata": []}
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            labels: dict[str, str] = {}
            samples: list[dict[str, object]] = []
            for ts_number, _ts_wire_type, ts_field in _fields(field):
                if ts_number == 1:
                    key, value = _parse_label(ts_field)
                    labels[key] = value
                elif ts_number == 2:
                    samples.append(_parse_sample(ts_field))
            result["timeseries"].append({"labels": labels, "samples": samples})
        elif number == 3:
            result["metadata"].append(_parse_metadata_v1(field))
    return result


def _parse_packed_uint32(buf: bytes) -> list[int]:
    refs: list[int] = []
    pos = 0
    while pos < len(buf):
        value, pos = _read_varint(buf, pos)
        refs.append(value)
    return refs


def _parse_metadata_v2(buf: bytes, symbols: list[str]) -> dict[str, object]:
    result: dict[str, object] = {}
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            result["type"] = field
        elif number == 3:
            result["help"] = symbols[field]
        elif number == 4:
            result["unit"] = symbols[field]
    return result


def _parse_v2(buf: bytes) -> dict[str, object]:
    symbols: list[str] = []
    timeseries_payloads: list[bytes] = []
    for number, _wire_type, field in _fields(buf):
        if number == 4:
            symbols.append(field.decode())
        elif number == 5:
            timeseries_payloads.append(field)
    result: dict[str, object] = {"symbols": symbols, "timeseries": []}
    for payload in timeseries_payloads:
        labels: dict[str, str] = {}
        samples: list[dict[str, object]] = []
        metadata: dict[str, object] = {}
        for number, _wire_type, field in _fields(payload):
            if number == 1:
                refs = _parse_packed_uint32(field)
                labels.update(
                    {
                        symbols[refs[i]]: symbols[refs[i + 1]]
                        for i in range(0, len(refs), 2)
                    }
                )
            elif number == 2:
                samples.append(_parse_sample(field))
            elif number == 5:
                metadata = _parse_metadata_v2(field, symbols)
        result["timeseries"].append(
            {"labels": labels, "samples": samples, "metadata": metadata}
        )
    return result


@dataclass
class Capture:
    path: Path


@dataclass(frozen=True)
class PrometheusRemoteWriteAssertions:
    requests: list[dict[str, object]] = field(default_factory=list)
    after: str | None = None


def _make_handler(capture: Capture):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            if self.headers.get("Content-Encoding") != "snappy":
                self.send_error(HTTPStatus.BAD_REQUEST, "missing snappy encoding")
                return
            version = self.headers.get("X-Prometheus-Remote-Write-Version", "")
            content_type = self.headers.get("Content-Type")
            expected_content_type = (
                "application/x-protobuf;proto=io.prometheus.write.v2.Request"
                if version == "2.0.0"
                else "application/x-protobuf"
            )
            if content_type != expected_content_type:
                self.send_error(HTTPStatus.BAD_REQUEST, "wrong content type")
                return
            decoded = _snappy_decompress(body)
            parsed = _parse_v2(decoded) if version == "2.0.0" else _parse_v1(decoded)
            parsed["content_type"] = content_type
            parsed["version"] = version
            with capture.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(parsed, sort_keys=True) + "\n")
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_: object) -> None:
            return

    return Handler


@fixture(name="prometheus_remote_write", assertions=PrometheusRemoteWriteAssertions)
def run() -> FixtureHandle:
    temp_dir = Path(tempfile.mkdtemp(prefix="prometheus-remote-write-"))
    capture = Capture(temp_dir / "requests.jsonl")
    capture.path.touch()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(capture))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    port = server.server_address[1]

    def teardown() -> None:
        server.shutdown()
        worker.join()
        server.server_close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    def assert_test(
        *, test: Path, assertions: PrometheusRemoteWriteAssertions, **_: Any
    ) -> None:
        if assertions.after is not None and test.name != assertions.after:
            return
        if not assertions.requests:
            return
        observed = [json.loads(line) for line in capture.path.read_text().splitlines()]
        if observed != assertions.requests:
            expected = json.dumps(assertions.requests, indent=2, sort_keys=True)
            actual = json.dumps(observed, indent=2, sort_keys=True)
            raise AssertionError(
                "Prometheus remote write capture mismatch\n"
                f"expected:\n{expected}\nactual:\n{actual}"
            )

    return FixtureHandle(
        env={
            "PROMETHEUS_REMOTE_WRITE_URL": f"http://127.0.0.1:{port}/api/v1/write",
            "PROMETHEUS_REMOTE_WRITE_CAPTURE": str(capture.path),
        },
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
