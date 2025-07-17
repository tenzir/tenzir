---
title: "Fix meta index nondeterminism"
type: bugfix
authors: tobim
pr: 842
---

For some queries, the index evaluated only a subset of all relevant partitions
in a non-deterministic manner. Fixing a violated evaluation invariant now
guarantees deterministic execution.
