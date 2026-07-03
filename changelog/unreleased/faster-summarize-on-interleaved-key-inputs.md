---
title: Faster summarize on interleaved-key inputs
type: change
authors:
  - aljazerzen
  - claude
created: 2026-07-03T09:41:05.983953Z
---

The `summarize` operator is significantly faster on inputs where consecutive
events belong to different groups (for example, a log file replayed in a loop
with interleaved source/destination pairs). Previously, aggregation state was
updated once per group-key *transition*, which on interleaved data degenerates
to one update per row. Each update pays the full cost of evaluating the
aggregation expression, so throughput scaled linearly with the number of
transitions rather than the number of distinct groups.

Aggregations are now updated exactly once per distinct group per incoming batch,
regardless of how many times the group key changes within that batch. A
measured benchmark with 1.2 million interleaved-key events and 10 aggregations
showed a 31× throughput improvement (~17 k events/s → ~520 k events/s).
