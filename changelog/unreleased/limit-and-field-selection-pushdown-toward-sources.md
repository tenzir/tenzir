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
hints. `export` also records the field selection, which you can inspect with
`tenzir --dump-opt-ir`; acting on it at the storage layer is not planned.

The `export` operator now honors the limit: it stops opening partitions once it
has enough matching events. Filters that require local evaluation
conservatively read everything.
The `subscribe` source now acts on these hints: it stops after the requested
number of matching events and drops unneeded top-level fields before forwarding
events. Nested field selections retain the containing record until `select`
applies the exact selection.

The `read_parquet` and `read_feather` operators honor both hints, including
inside an explicit `from_file` subpipeline. They retain filter-required fields,
skip decoding unrelated top-level columns, and stop after enough matching rows.
Feather supports this for IPC files and streams, including concatenated streams
with different column orders. Filters containing function calls conservatively
retain all columns.

Parquet still buffers its full byte input before reading the footer. These
optimizations reduce decoding work, not the bytes fetched from the source;
random-access file I/O is not part of `read_parquet`.
