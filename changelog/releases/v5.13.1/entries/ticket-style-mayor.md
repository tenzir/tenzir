---
title: "Buffering in the `fork` operator"
type: bugfix
author: raxyte
created: 2025-08-22T09:08:01Z
pr: 5436
---

We fixed an issue in the `fork` operator where the last event would get stuck.
