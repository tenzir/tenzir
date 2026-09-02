# runner: python
# timeout: 20

from read_avro_test_utils import assert_completes, encode_bytes, make_container


def main() -> None:
    for codec in ["null", "deflate"]:
        container = make_container(
            codec,
            b'{"type":"string"}',
            [
                (1, encode_bytes(b"hello")),
                (0, b""),
                (1, encode_bytes(b"world")),
            ],
        )
        assert_completes(
            container,
            'from_stdin { read_avro schema={type: "string"} } | write_ndjson',
            [{"value": "hello"}, {"value": "world"}],
        )
        if codec == "null":
            assert_completes(
                container + container,
                'from_stdin { read_avro schema={type: "long"} } | write_ndjson',
                [
                    {"value": "hello"},
                    {"value": "world"},
                    {"value": "hello"},
                    {"value": "world"},
                ],
                (
                    "warning: provided Avro schema differs from container writer schema",
                    'read_avro schema={type: "long"}',
                    "~",
                    "using the writer schema embedded in the Avro container",
                ),
            )
        ignored_schema = '{type: "invalid"}' if codec == "null" else '"not valid JSON"'
        assert_completes(
            container,
            f"from_stdin {{ split_bytes 1 | read_avro schema={ignored_schema} }}"
            " | write_ndjson",
            [{"value": "hello"}, {"value": "world"}],
        )
    empty_deflate = b"\x03\x00"
    assert_completes(
        make_container(
            "deflate",
            b'"null"',
            [(0, empty_deflate)] * 1_024 + [(1, empty_deflate)],
            encoded_payloads=True,
        ),
        "from_stdin { read_avro } | summarize count=count() | write_ndjson",
        [{"count": 1}],
    )
    noncanonical_empty_deflate = b"\x01\x00\x00\xff\xff"
    assert_completes(
        make_container(
            "deflate",
            b'"null"',
            [(0, noncanonical_empty_deflate), (1, noncanonical_empty_deflate)],
            encoded_payloads=True,
        ),
        "from_stdin { read_avro } | write_ndjson",
        [{"value": None}],
    )
    print("container_codecs: true")


if __name__ == "__main__":
    main()
