# runner: python
# timeout: 60

from parquet_test_utils import assert_roundtrip

assert_roundtrip("write_parquet")
