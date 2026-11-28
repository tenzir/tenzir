---
title: Batch-independent results for XSV parsing and printing functions
type: bugfix
authors:
  - mavam
created: 2026-11-28T13:32:38.402923Z
---

The `parse_csv`, `parse_tsv`, `parse_ssv`, and `parse_xsv` functions with `auto_expand=true`, and the `print_csv`, `print_tsv`, `print_ssv`, and `print_xsv` functions, now compute the result for each event only from the event itself. Previously, the result could depend on the other events in the same batch, so the same event could produce different values depending on batching or on where the optimizer placed a filter.

With `auto_expand=true`, an event only gets `unnamed` fields for its own excess values. Previously, a shorter event after a longer one in the same batch also received the longer event's extra fields with `null` values:

```tql
from {x: "1,2"}, {x: "3"}
batch
select y = x.parse_csv(header="a", auto_expand=true)
```

```tql
{y: {a: 1, unnamed1: 2}}
{y: {a: 3}}
```

A `null` nested record now prints as a single `null_value`, like any other `null` value. Previously, it printed as one `null_value` per field that the nested record had in other events of the batch:

```tql
from {x: {a: 1, b: {c: 2, d: 3}}}, {x: {a: 4, b: null}}
batch
select y = x.print_csv()
```

```tql
{y: "1,2,3"}
{y: "4,"}
```

Previously, the second event printed as `"4,,"` in this pipeline, but as `"4,"` when it ended up in a batch of its own.

The `read_csv`, `read_tsv`, `read_ssv`, and `read_xsv` operators and the matching `write_*` operators keep their behavior, because they share one header across all lines of their input or output.
