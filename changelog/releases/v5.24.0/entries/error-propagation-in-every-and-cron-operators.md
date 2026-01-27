---
title: Error propagation in every and cron operators
type: bugfix
authors:
  - raxyte
pr: 5632
created: 2025-12-23T09:38:19.320195Z
---

The `every` and `cron` operators now correctly propagate errors from their subpipelines instead of silently swallowing them.
