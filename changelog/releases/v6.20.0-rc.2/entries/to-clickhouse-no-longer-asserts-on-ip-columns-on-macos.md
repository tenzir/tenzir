---
title: "`to_clickhouse` no longer crashes on IP addresses and subnets on macOS"
type: bugfix
authors:
  - mavam
created: 2026-09-24T17:08:44.809015Z
---

On macOS, `to_clickhouse` no longer fails an internal assertion when writing
fields of type `ip` or `subnet`.
