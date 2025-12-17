---
title: "TQL2 support in compaction plugin"
type: change
author: jachris
created: 2025-06-25T19:13:24Z
pr: 5302
---

The pipelines defined as part of the compaction configuration can now use TQL2.
For backwards-compatibility, TQL1 pipelines still work, but they are deprecated
and emit a warning on start-up.
