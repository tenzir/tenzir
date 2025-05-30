---
title: "Improve import batching options"
type: bugfix
authors: dominiklohmann
pr: 1058
---

Stalled sources that were unable to generate new events no longer stop import
processes from shutting down under rare circumstances.
