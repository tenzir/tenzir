---
title: "Improve name lookup in meta index to reduce FPs"
type: bugfix
authors: dominiklohmann
pr: 2086
---

A performance bug in the first stage of query evaluation caused VAST to return
too many candidate partitions when querying for a field suffix. For example, a
query for the `ts` field commonly used in Zeek logs also included partitions for
`netflow.pkts` from `suricata.netflow` events. This bug no longer exists,
resulting in a considerable speedup of affected queries.
