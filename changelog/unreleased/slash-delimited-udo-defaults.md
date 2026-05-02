---
title: Slash-delimited UDO defaults
type: bugfix
authors:
  - mavam
  - codex
pr: 6108
created: 2026-05-02T14:43:20.745759Z
---

Package UDOs now load correctly when a typed string default looks like a TQL pattern, such as `default: "/tmp-data/"`.

Previously, loading such a package could abort with an unexpected internal error before any pipeline ran.
