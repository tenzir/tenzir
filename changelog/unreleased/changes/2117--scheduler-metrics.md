We revised the query scheduling logic to exploit synergies when multiple queries
run at the same time. In that vein, we updated the related metrics with more
accurate names to reflect the new mechanism.

* `query.workers.idle` -> `scheduler.partition.remaining-capacity`
* `query.workers.busy` -> `scheduler.partition.current-lookups`
* `query.backlog.normal` -> removed
* `query.backlog.low` -> removed
* `scheduler.queries.pending` -> overall number of currently active queries
* `scheduler.partition.pending` -> number of partitions to evaluate in order to
                                   complete all currently pending queries
* `scheduler.partition.materializations` -> number of partitions that were
                                            loaded from disk since the last time
                                            this metric was issued.
* `scheduler.partition.schedulings` -> number of partitions that were used to
*                                      evaluate queries since the last time
                                       this metric was issued.
* `scheduler.partition.lookups` -> number of queries that were sent to
                                   partitions since the last time this metric
                                   was issued.

We also removed the keys `index.backlog.num-normal-priority` and
`index.backlog.num-low-priority` from the output of `vast status`.
