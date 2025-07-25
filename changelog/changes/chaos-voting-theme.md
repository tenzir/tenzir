---
title: "Handle unsupported Arrow types in `read_parquet`"
type: bugfix
authors: IyeOnline
pr: 5373
---

The Tenzir and the `read_parquet` operator only support a subset of the Apache
Arrow type system. Reading an unsupported parquet file could previously crash
Tenzir in some situations. This is now fixed and the operator instead raises
an error.
