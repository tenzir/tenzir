---
title: Consistent value formatting in direct JSON sink output
type: bugfix
authors:
  - tobim
created: 2026-07-30T15:48:00.252991Z
---

Sink operators that serialize events to JSON internally, such as `to_kafka` and `to_amqp`, now format `time`, `duration`, `ip`, and `subnet` values identically to `write_json` and `write_ndjson`. Previously, timestamps rendered as `2026-07-30 12:00:00.000000000` instead of the ISO 8601 form `2026-07-30T12:00:00Z`.
