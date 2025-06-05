---
title: "Fix a stack-use-after-move in `save_tcp`"
type: bugfix
authors: dominiklohmann
pr: 5103
---

The `save_tcp` operator no longer panics or crashes on startup when it cannot
connect to the provided hostname and port, and instead produces a helpful error
message.
