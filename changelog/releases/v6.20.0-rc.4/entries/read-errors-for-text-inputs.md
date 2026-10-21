---
title: Read errors for text inputs
type: bugfix
authors:
  - mavam
created: 2026-10-20T07:08:58.772137Z
---

Loading pipeline files, configuration files, and other text inputs now reports read errors instead of silently accepting empty or partial content. For example, `tenzir -f /path/to/directory` now reports a read error rather than treating the directory as an empty pipeline. Process substitutions and inputs such as `--config=/dev/null` remain supported.
