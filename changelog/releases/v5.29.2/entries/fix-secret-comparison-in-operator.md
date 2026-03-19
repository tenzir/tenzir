---
title: Fix secret comparison bypass in `in` operator fast path
type: bugfix
authors:
  - jachris
pr: 5899
created: 2026-03-12T00:00:00Z
---

The `in` operator fast path now correctly prevents comparison of secret values.
Previously, `secret_value in [...]` would silently compare instead of returning
null with a warning, bypassing the established secret comparison policy.
