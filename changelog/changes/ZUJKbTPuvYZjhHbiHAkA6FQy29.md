---
title: "Stabilize the `bitz` format"
type: bugfix
authors: dominiklohmann
pr: 4633
---

We fixed a very rare crash in the zero-copy parser implementation of `read
feather` and `read parquet` that was caused by releasing shared memory too
early.
