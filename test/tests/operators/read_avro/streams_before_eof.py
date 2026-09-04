# runner: python
# timeout: 20

from read_avro_test_utils import assert_streams, encode_bytes, make_container


def main() -> None:
    assert_streams(
        b"\x0ahello",
        'from_stdin { read_avro schema=r#"{"type":"string"}"# }'
        " | head 1 | write_ndjson",
        {"value": "hello"},
    )
    for codec in ["null", "deflate"]:
        assert_streams(
            make_container(
                codec,
                b'{"type":"string"}',
                [(1, encode_bytes(b"hello"))],
            ),
            "from_stdin { read_avro } | head 1 | write_ndjson",
            {"value": "hello"},
        )
    print("streams_before_eof: true")


if __name__ == "__main__":
    main()
