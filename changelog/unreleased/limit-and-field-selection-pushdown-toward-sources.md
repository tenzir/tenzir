---
title: Limit and field selection pushdown toward sources
type: feature
authors:
  - mavam
created: 2026-09-04T19:04:00.845198Z
---

The pipeline optimizer now propagates the row limit of `head` and the field
selection of `select` toward the source of a pipeline, alongside the predicate
pushdown it already performs for `where`. Sources that understand these hints
can stop reading early and skip fields that the pipeline never uses.

For example, in

```tql
export
where severity == "high"
select id, message
head 10
```

the source learns that at most 10 matching events are needed and that only
`id`, `message`, and `severity` are read. The `head` and `select` operators stay
in the pipeline, so results are unchanged whether or not a source acts on the
hints. In this release, `export` records the hints, which you can inspect with
`tenzir --dump-opt-ir`. Acting on them at the storage layer follows in a later
release.
