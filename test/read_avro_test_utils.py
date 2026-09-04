from __future__ import annotations

import json
import os
import select
import shlex
import subprocess
import zlib
from collections.abc import Iterable


def encode_long(value: int) -> bytes:
    encoded = (value << 1) ^ (value >> 63)
    result = bytearray()
    while encoded > 0x7F:
        result.append((encoded & 0x7F) | 0x80)
        encoded >>= 7
    result.append(encoded)
    return bytes(result)


def encode_bytes(value: bytes) -> bytes:
    return encode_long(len(value)) + value


def make_container(
    codec: str,
    schema: bytes,
    blocks: Iterable[tuple[int, bytes]],
    *,
    encoded_payloads: bool = False,
) -> bytes:
    sync = bytes(range(16))
    metadata = (
        encode_long(2)
        + encode_bytes(b"avro.schema")
        + encode_bytes(schema)
        + encode_bytes(b"avro.codec")
        + encode_bytes(codec.encode())
        + encode_long(0)
    )
    result = bytearray(b"Obj\x01" + metadata + sync)
    for count, payload in blocks:
        if codec == "deflate" and not encoded_payloads:
            compressor = zlib.compressobj(wbits=-15)
            payload = compressor.compress(payload) + compressor.flush()
        result += encode_long(count) + encode_long(len(payload)) + payload + sync
    return bytes(result)


def assert_streams(payload: bytes, pipeline: str, expected: object) -> None:
    process = subprocess.Popen(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.stdin is not None
        assert process.stdout is not None
        process.stdin.write(payload)
        process.stdin.flush()
        readable, _, _ = select.select([process.stdout], [], [], 5)
        assert readable, "read_avro produced no output before EOF"
        line = process.stdout.readline()
        assert json.loads(line) == expected, line
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def assert_completes(
    payload: bytes,
    pipeline: str,
    expected: list[object],
    expected_stderr: tuple[str, ...] = (),
) -> None:
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", pipeline],
        input=payload,
        capture_output=True,
        timeout=10,
        check=False,
    )
    stderr = result.stderr.decode()
    assert result.returncode == 0, stderr
    expected_warning_count = sum(
        fragment.startswith("warning:") for fragment in expected_stderr
    )
    assert stderr.count("warning:") == expected_warning_count, stderr
    assert "error:" not in stderr, stderr
    for fragment in expected_stderr:
        assert fragment in stderr, stderr
    assert expected_stderr or not stderr, stderr
    actual = [json.loads(line) for line in result.stdout.splitlines()]
    assert actual == expected, actual


def assert_rejected(payload: bytes, expected: str) -> None:
    result = subprocess.run(
        [
            *shlex.split(os.environ["TENZIR_BINARY"]),
            "--bare-mode",
            "from_stdin { read_avro }",
        ],
        input=payload,
        capture_output=True,
        timeout=5,
        check=False,
    )
    stderr = result.stderr.decode()
    assert result.returncode != 0, stderr
    assert expected in stderr, stderr
