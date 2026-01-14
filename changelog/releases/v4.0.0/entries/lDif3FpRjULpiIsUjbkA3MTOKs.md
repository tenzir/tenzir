---
title: "Add diagnostics (and some other improvements)"
type: change
author: jachris
created: 2023-06-20T11:09:14Z
pr: 3223
---

We changed the default connector of `read <format>` and `write <format>` for
all formats to `stdin` and `stdout`, respectively.

We removed language plugins in favor of operator-based integrations.

The interface of the operator, loader, parser, printer and saver plugins was
changed.
