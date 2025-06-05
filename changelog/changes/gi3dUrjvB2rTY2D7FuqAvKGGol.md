---
title: "Fix mismatch in type metadata after assignments"
type: bugfix
authors: dominiklohmann
pr: 5033
---

We fixed a bug that caused a loss of type names for nested fields in
assignments, causing field metadata in `write_feather` and `write_parquet` to be
missing.
