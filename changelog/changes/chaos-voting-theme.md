---
title: "Handle unsupported Arrow types in `read_parquet`"
type: bugfix
authors: IyeOnline
pr: 5373
---

Tenzir and the `read_parquet` operator only support a subset of all Parquet types.
Reading an unsupported parquet file could previously crash Tenzir in some
situations. This is now fixed and the operator instead raises an error.
