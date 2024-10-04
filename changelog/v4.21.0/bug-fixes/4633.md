We fixed a very rare crash in the zero-copy parser implementation of `read
feather` and `read parquet` that was caused by releasing shared memory too
early.
