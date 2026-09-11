---
title: "`from_kafka` with `exit` no longer stops early on a rebalance"
type: bugfix
authors:
  - aljazerzen
---

`from_kafka ..., exit=true` stopped as soon as it observed an empty partition
assignment. Kafka's default rebalance protocol revokes every partition before
assigning the new set, so any rebalance — triggered whenever another consumer
joins or leaves the group — briefly left the operator without partitions and
ended the run before the topic was read to its end.

The operator now distinguishes the two halves of a rebalance and only concludes
from an assignment, never from a revocation — and only while that assignment is
still the current one, so a rebalance that happens between observing an
assignment and acting on it cannot end the run either. A genuinely empty
assignment, such as when a parallel pipeline runs more instances than the topic
has partitions, still finishes immediately.
