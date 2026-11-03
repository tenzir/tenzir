---
title: Package operator diagnostics during node startup
type: bugfix
authors:
  - zedoraps
created: 2026-11-03T17:42:11.425887Z
---

Node startup now warns about package operator syntax errors with their file,
line, and source excerpt. This makes it easier to identify the cause of later
`module not found` errors. Startup continues when the unavailable operators are
unused; pipelines that reference them still fail validation.
