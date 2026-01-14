---
title: "Eploit synergies when evaluating many queries at the same time"
type: change
author: tobim
created: 2022-04-08T13:10:57Z
pr: 2117
---

We revised the query scheduling logic to exploit synergies when multiple
queries run at the same time. In that vein, we updated the related metrics with
more accurate names to reflect the new mechanism. The new keys
`scheduler.partition.materializations`, `scheduler.partition.scheduled`, and
`scheduler.partition.lookups` provide periodic counts of partitions loaded from
disk and scheduled for lookup, and the overall number of queries issued to
partitions, respectively. The keys `query.workers.idle`, and
`query.workers.busy` were renamed to `scheduler.partition.remaining-capacity`,
and `scheduler.partition.current-lookups`. Finally, the key
`scheduler.partition.pending` counts the number of currently pending
partitions. It is still possible to opt-out of the new scheduling algorithm
with the (deprecated) option `--use-legacy-query-scheduler`.
