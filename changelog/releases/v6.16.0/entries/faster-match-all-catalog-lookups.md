---
title: Faster match-all catalog lookups
type: change
authors:
  - tobim
created: 2026-08-31T12:00:21.708966Z
---

Catalog lookups for queries that match all events, such as an unfiltered
`export` or the candidate selection of a rebuild, no longer evaluate the query
against every partition's synopses. On state directories with many partitions
this makes such lookups start noticeably faster.
