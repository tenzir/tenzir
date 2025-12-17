---
title: "Stabilize the `bitz` format"
type: bugfix
author: dominiklohmann
created: 2024-10-01T14:25:20Z
pr: 4633
---

We fixed a very rare crash in the zero-copy parser implementation of `read
feather` and `read parquet` that was caused by releasing shared memory too
early.
