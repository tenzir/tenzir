VAST now stores its partitions per layout. The new option `vast.partition-timeout`
controls the time after which a partition is flushed to disk, allowing for the
`vast.max-partition-size` option to be overruled. The partition timeout defaults
to 1 hour.
