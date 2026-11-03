---
title: Package operator diagnostics during node startup
type: bugfix
authors:
  - zedoraps
created: 2026-11-03T16:43:37.830719Z
---

Node startup now reports package operator syntax errors with their file, line,
and source excerpt. A statically configured package with invalid operators stops
startup before pipeline validation, avoiding secondary `module not found`
errors. Previously installed runtime packages that fail to load remain
removable, while healthy packages remain usable.
