# runner: python

from parquet_test_utils import assert_roundtrip

assert_roundtrip('write_parquet compression_type="zstd", compression_level=3')
