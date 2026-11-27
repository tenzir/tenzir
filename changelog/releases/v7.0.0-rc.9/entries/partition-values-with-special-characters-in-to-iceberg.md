---
title: Partition values with special characters in to_iceberg
type: bugfix
authors:
  - zedoraps
created: 2026-11-26T11:44:47.355463Z
---

The `to_iceberg` operator now writes data files that `from_iceberg` and other engines can read when a partition value contains a character that the partition path percent-encodes, such as a space or a slash. The operator stores such a file under the literal location that the table metadata records, for example `…/k=a%20b/…`. Files that an earlier version stored under the decoded name remain readable.
