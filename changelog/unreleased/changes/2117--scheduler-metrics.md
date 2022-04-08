We revised the query scheduling logic to exploit synergies when multiple queries
run at the same time. In that vein, we updated the related metrics with more
accurate names to reflect the new mechanism. The new keys
`scheduler.partition.materializations`, `scheduler.partition.schedulings`, and
`scheduler.partition.lookups`  issue periodic counts of partitions loaded from
disk and scheduled for lookup, and the overall number of queries issued to
partitions respectively. The keys `query.workers.idle`, and `query.workers.busy`
were renamed to `scheduler.partition.remaining-capacity`, and
`scheduler.partition.current-lookups`. Finally, the key
`scheduler.partition.pending` counts the number of currently pending partitions.
It is still possible to opt-out of the new scheduling algorithm with the option
`--use-legacy-query-scheduler`, but that is immediately deprecated and will not
be included in the next minor release.
