The new `flush` command causes VAST to decommission all currently active
partitions, i.e., write all active partitions to disk immediately regardless of
their size or the active partition timeout. This is particularly useful for
testing, or when needing to guarantee in automated scripts that input is
available for operations that only work on persisted passive partitions. The
`flush` command returns only after all active partitions were flushed to disk.
