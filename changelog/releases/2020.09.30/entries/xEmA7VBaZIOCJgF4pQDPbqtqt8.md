---
title: "Improve import batching options"
type: bugfix
author: dominiklohmann
created: 2020-09-16T08:03:17Z
pr: 1058
---

Stalled sources that were unable to generate new events no longer stop import
processes from shutting down under rare circumstances.
