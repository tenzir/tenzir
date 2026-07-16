---
title: Cleaner source builds on macOS
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6441
created: 2026-07-13T06:33:03.366701Z
---

Source builds on macOS no longer print avoidable warnings from platform-specific code, fmt 12 integration, or bundled dependencies. This makes new compiler diagnostics easier to spot.
