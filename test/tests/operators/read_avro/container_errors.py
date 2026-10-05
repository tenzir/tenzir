# runner: python
# timeout: 60

from read_avro_test_utils import assert_rejected, make_container


def main() -> None:
    excessive_depth = b'"null"'
    for _ in range(101):
        excessive_depth = b'{"type":"array","items":' + excessive_depth + b"}"
    assert_rejected(
        make_container("null", excessive_depth, []),
        "Avro schema exceeds the maximum supported nesting depth",
    )
    print("container_errors: true")


if __name__ == "__main__":
    main()
