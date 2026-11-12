---
title: Time zone support for parse_time
type: feature
authors:
  - mavam
created: 2026-11-06T11:58:11.804242Z
---

The `parse_time` function has a new `timezone` option that interprets the parsed
wall-clock time in an IANA time zone, such as `Europe/Zurich`, and returns the
corresponding UTC time. Daylight saving time is taken into account.

This is useful for RFC 3164 syslog timestamps, which carry the sender's local
time without a year or time zone. Because `timezone` accepts any expression, you
can resolve the zone per event, for example from an asset inventory:

```tql
timestamp = timestamp.parse_time("%b %e %H:%M:%S", reference=now(), timezone=host.timezone)
```

Local times that a daylight saving switch repeats resolve to the earlier
instant, and local times that it skips shift forward by the length of the gap.
An explicit UTC offset in the input, parsed with `%z`, takes precedence over
`timezone`. Unknown time zone names produce a warning and a null result, and a
null `timezone` keeps the previous behavior of reading the time as UTC.
