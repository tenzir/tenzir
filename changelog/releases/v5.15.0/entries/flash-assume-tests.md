---
title: "Explicit Commits in `load_kafka`"
type: bugfix
author: IyeOnline
created: 2025-09-18T13:59:28Z
pr: 5465
---

The `load_kafka` operator now explicitly commits messages it has consumed.
By default, it will commit every 1000 messages or every 10 seconds, with the
behavior being customizable via two new operator arguments.

Previously, the operator would commit every message asynchronously loaded by the
backing library automatically, which may have included messages that were never
accepted by the pipeline.
