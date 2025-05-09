The `tenzir.zstd-compression-level` option now works again as advertised for
setting the Zstd compression level for the partitions written by the `import`
operator. For the past few releases, newly written partitions unconditionally
used the default compression level.
