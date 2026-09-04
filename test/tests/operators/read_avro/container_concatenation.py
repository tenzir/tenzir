# runner: python
# timeout: 20

from read_avro_test_utils import (
    assert_completes,
    encode_bytes,
    encode_long,
    make_container,
)


def main() -> None:
    assert_completes(
        make_container(
            "null",
            b'["null",{"type":"map","values":"long"}]',
            [
                (1, bytes.fromhex("020202780200")),
                (1, bytes.fromhex("00")),
                (1, bytes.fromhex("020202780400")),
            ],
        ),
        "from_stdin { read_avro }"
        " | kind = type_of(value).kind | select kind | write_ndjson",
        [{"kind": "record"}, {"kind": "record"}, {"kind": "record"}],
    )
    first = make_container(
        "null",
        b'{"type":"record","name":"first","fields":[{"name":"a","type":"long"}]}',
        [(1, encode_long(1))],
    )
    second = make_container(
        "deflate",
        b'{"type":"record","name":"second","fields":[{"name":"b","type":"string"}]}',
        [(1, encode_bytes(b"x"))],
    )
    assert_completes(
        first + second,
        "from_stdin { split_bytes 1 | read_avro } | write_ndjson",
        [{"a": 1}, {"b": "x"}],
    )
    empty = make_container("null", b'"string"', [])
    assert_completes(
        (empty * 1_024)
        + make_container("null", b'"string"', [(1, encode_bytes(b"value"))]),
        "from_stdin { read_avro } | write_ndjson",
        [{"value": "value"}],
    )
    print("container_concatenation: true")


if __name__ == "__main__":
    main()
