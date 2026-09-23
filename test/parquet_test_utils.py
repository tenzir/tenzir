"""Helpers for checking interoperability of Parquet readers and writers."""

from format_roundtrip_test_utils import assert_format_roundtrip


def assert_roundtrip(writer: str) -> None:
    rows = assert_format_roundtrip(writer, ["read_parquet"])
    print(f"{{\n  n: {len(rows)},\n  total: {sum(row['x'] for row in rows)},\n}}")
