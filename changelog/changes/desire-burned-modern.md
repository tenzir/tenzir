---
title: "Improved `export` memory management"
type: change
authors: jachris
pr: 5520
---

The database partitions opened by the `export` operator previously read and
forwarded their entire contents to `export` without waiting for the operator to
forward them. This circumvented the usual backpressure mechanism and could lead
to unexpectedly high memory usage. Now, the backpressure is propagated to the
underlying storage layer.
