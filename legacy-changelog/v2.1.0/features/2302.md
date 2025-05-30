VAST emits the new metric `partition.events-written` when writing a partition to
disk. The metric's value is the number of events written, and the
`metadata_schema` field contains the name of the partition's schema.
