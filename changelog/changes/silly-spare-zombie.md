---
title: "TQL2 support in compaction plugin"
type: change
authors: jachris
pr: 5302
---

The pipelines defined as part of the compaction configuration can now use TQL2.
For backwards-compatability, TQL1 pipelines still work, but they are deprecated
and emit a warning on start-up.
