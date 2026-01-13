---
title: Duplicate diagnostics only suppressed for 4 hours
type: change
authors:
  - raxyte
  - claude
pr: 5652
created: 2026-01-12T12:19:02.991346Z
---

Repeated warnings and errors now resurface every 4 hours instead of being suppressed forever. Previously, once a diagnostic was shown, it would never appear again even if the underlying issue persisted. This change helps users notice recurring problems that may require attention.
