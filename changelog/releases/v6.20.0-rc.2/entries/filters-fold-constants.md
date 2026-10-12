---
title: Filters fold constants
type: change
authors:
  - mavam
created: 2026-09-30T13:05:46.414968Z
---

The optimizer now simplifies `where` filters. It evaluates parts of a predicate that do not depend on the event, drops predicates that always hold, and prunes `and`/`or` branches that cannot match. For example, `class_uid = 3002 | where class_uid == 3002 and src_ip == 1.2.3.4` sends only `src_ip == 1.2.3.4` to the source, and `where class_uid == 4001` keeps no events at all.
