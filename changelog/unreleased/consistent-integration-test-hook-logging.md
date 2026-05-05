---
title: Consistent integration-test hook logging
type: bugfix
authors:
  - mavam
  - codex
created: 2026-05-05T09:19:14.843843Z
---

Debug output from Tenzir's integration-test hooks now uses the same formatting as other `tenzir-test --debug` messages. Previously, diagnostics about local Tenzir binaries could appear without the standard debug marker, making traces harder to scan.
