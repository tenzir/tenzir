# runner: python
# timeout: 20

from read_avro_test_utils import (
    assert_rejected,
    encode_bytes,
    encode_long,
    make_container,
)


def main() -> None:
    for keys, error in [
        ([b"x", b"x"], "duplicate Avro map key"),
        ([b"\xff"], "invalid UTF-8 in Avro map key"),
    ]:
        payload = (
            encode_long(len(keys))
            + b"".join(encode_bytes(key) + encode_long(1) for key in keys)
            + encode_long(0)
        )
        assert_rejected(
            make_container("null", b'{"type":"map","values":"long"}', [(1, payload)]),
            error,
        )
    excessive_depth = b'"null"'
    for _ in range(101):
        excessive_depth = b'{"type":"array","items":' + excessive_depth + b"}"
    assert_rejected(
        make_container("null", excessive_depth, []),
        "Avro schema exceeds the maximum supported nesting depth",
    )
    assert_rejected(
        make_container("snappy", b'"null"', []),
        "unsupported Avro container codec `snappy`",
    )
    assert_rejected(
        make_container(
            "deflate",
            b'"null"',
            [(0, b"\x01\x01\x00\xfe\xff\x00")],
            encoded_payloads=True,
        ),
        "Avro container block contains unexpected decoded data",
    )
    for payload, error in [
        (b"\x03", "invalid Avro deflate block"),
        (b"\x03\x00\x00", "trailing data in Avro deflate block"),
    ]:
        assert_rejected(
            make_container(
                "deflate",
                b'"null"',
                [(0, payload)],
                encoded_payloads=True,
            ),
            error,
        )
    sync_mismatch = bytearray(make_container("null", b'"null"', [(1, b"")]))
    sync_mismatch[-1] ^= 1
    assert_rejected(
        bytes(sync_mismatch),
        "Avro container sync marker mismatch",
    )
    print("container_errors: true")


if __name__ == "__main__":
    main()
