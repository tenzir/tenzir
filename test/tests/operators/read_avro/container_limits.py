# runner: python
# timeout: 20

import json

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
        (bytes(wide_map) + (b"\x00" * 127)) * 8,
        'from_stdin { read_avro schema={type: ["null", '
        '{type: "map", values: "long"}]} } | measure'
        " | summarize max_rows=max(events), total_rows=sum(events)"
        " | oversized=max_rows > 500"
        " | select total_rows, oversized | to_stdout { write_ndjson }",
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
    variants = [
        {
            "type": "record",
            "name": f"Variant{variant}",
            "fields": [
                {"name": f"f{variant}_{field}", "type": "long"} for field in range(8)
            ],
        }
        for variant in range(256)
    ]
    schema = {"type": "array", "items": variants}
    items = [encode_long(variant) + b"\x00" * 8 for variant in range(256)]
    wide_list = encode_long(len(items) * 8) + b"".join(items) * 8 + encode_long(0)
    # Encoded values are small, but the merged record columns require padding
    # for every element, including fields belonging to other union branches.
    assert_rejected(
        make_container("null", json.dumps(schema).encode(), [(1, wide_list)]),
        "decoded Avro datum exceeds 16 MiB",
    )
    raw_reader = (
        "from_stdin { read_avro schema="
        + json.dumps(json.dumps(schema, separators=(",", ":")))
        + " }"
    )
    assert_rejected(wide_list, "decoded Avro datum exceeds 16 MiB", raw_reader)
    nested_schema = {"type": "array", "items": schema}
    assert_rejected(
        make_container(
            "null",
            json.dumps(nested_schema).encode(),
            [(1, encode_long(1) + wide_list + encode_long(0))],
        ),
        "decoded Avro datum exceeds 16 MiB",
    )
    # The same child columns also accumulate across lists in separate events.
    rows = b"".join(encode_long(8) + item * 8 + encode_long(0) for item in items)
    measure = (
        " | measure | summarize total_rows=sum(events), batches=count()"
        " | split=batches > 1 | select total_rows, split"
        " | to_stdout { write_ndjson }"
    )
    assert_completes(
        make_container("null", json.dumps(schema).encode(), [(len(items), rows)]),
        "from_stdin { read_avro }" + measure,
        [{"total_rows": 256, "split": True}],
    )
    assert_completes(
        rows,
        raw_reader + measure,
        [{"total_rows": 256, "split": True}],
    )
    print("container_limits: true")


if __name__ == "__main__":
    main()
