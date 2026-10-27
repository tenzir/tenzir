---
title: Fewer data files from to_iceberg
type: change
authors:
  - zedoraps
created: 2026-10-27T10:36:40.446088Z
---

The `to_iceberg` operator now writes every column from the current table schema
into each data file, filling missing columns with typed nulls. Mixed event
schemas can share a file when previously empty columns gain values, reducing
small files.

The operator also retains buffered batches when nullable fields are
added, allowing several schema additions to share one data file. Open streaming
files still close on a schema change. Type changes and partition changes also
close the affected buffers before adopting the new layout.
