# runner: python
# timeout: 20

from read_avro_test_utils import (
    assert_completes,
    assert_rejected,
    encode_bytes,
    encode_long,
    make_container,
)


def main() -> None:
    wide_map = bytearray(encode_long(1) + encode_long(4_096))
    for index in range(4_096):
        wide_map += encode_bytes(str(index).encode()) + encode_long(0)
    wide_map += encode_long(0)
    assert_completes(
        bytes(wide_map) + (b"\x00" * 1_023),
        'from_stdin { read_avro schema={type: ["null", '
        '{type: "map", values: "long"}]} } | measure'
        " | summarize max_rows=max(events), total_rows=sum(events)"
        " | oversized=max_rows > 500"
        " | select total_rows, oversized | write_ndjson",
        [{"total_rows": 1_024, "oversized": False}],
    )
    assert_rejected(
        make_container("null", b'"null"', [((1 << 63) - 1, b"")]),
        "invalid Avro container object count",
    )
    assert_rejected(
        make_container(
            "deflate",
            b'{"type":"string"}',
            [(1, encode_bytes(b"x" * (16 * 1024 * 1024)))],
        ),
        "decoded Avro container block exceeds 16 MiB",
    )
    print("container_limits: true")


if __name__ == "__main__":
    main()
