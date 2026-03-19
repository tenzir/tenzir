---
title: Support long syslog structured-data parameter names
type: bugfix
authors:
  - mavam
  - codex
created: 2026-03-19T06:47:04.188426Z
---

The `read_syslog` operator and `parse_syslog` function now accept RFC 5424 structured-data parameter names longer than 32 characters, which some vendors emit despite the specification limit.

For example, this message now parses successfully instead of being rejected:

```text
<134>1 2026-03-18T11:00:51.194137+01:00 HOSTNAME abc 9043 23003147 [F5@12276 thx_f5_for_ignoring_the_32_char_limit_in_structured_data="thx"] broken example
```

This improves interoperability with vendor syslog implementations that exceed the RFC limit for structured-data parameter names.
