---
title: Exact fractional seconds in parsed times
type: bugfix
authors:
  - mavam
created: 2026-11-17T21:42:20.000000Z
---

Time literals and parsed time strings could be off by a nanosecond, as their fractional seconds went through a floating-point number that holds them only approximately. For example, `2024-01-01T00:00:00.000001` meant `2024-01-01T00:00:00.000000999`. Such times now mean exactly what they spell, in literals, in `time()`, and in parsers such as `read_syslog`.
