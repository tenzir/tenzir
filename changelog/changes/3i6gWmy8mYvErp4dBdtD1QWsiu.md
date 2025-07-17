---
title: "Add --append and --real-time to directory saver"
type: feature
authors: mavam
pr: 3379
---

The `directory` saver now supports the two arguments `-a|--append` and
`-r|--realtime` that have the same semantics as they have for the `file` saver:
open files in the directory in append mode (instead of overwriting) and flush
the output buffers on every update.
