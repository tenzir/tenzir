---
title: Cisco syslog headers and PRI-prefixed text
type: feature
authors:
  - zedoraps
created: 2026-09-11T10:17:41.978637Z
---

The `read_syslog` operator and `parse_syslog` function now accept Cisco ASA
headers and Cisco headers with a hostname before the timestamp. They also
accept text with only a syslog priority prefix when explicitly enabled:

```tql
from {line: "<166>Sep 01 2026 16:46:57: %ASA-6-302014: message"},
     {line: "<45>: HOSTNAME-123: Jan 14 05:25:06.737: %FW-6-SESS_AUDIT_TRAIL: message"},
     {line: "<45>message"}
this = line.parse_syslog(accept_pri_only=true)
```

The third record produces `facility: 5`, `severity: 5`, and `content: "message"`,
with null header fields. Standard syslog headers take precedence over this
fallback. The option defaults to `false`, so malformed headers do not silently
become PRI-only records. Use `read_syslog accept_pri_only=true` for streams
that intentionally contain this format. Enabling the option also accepts
malformed headers with a valid PRI prefix as text, without a parsing warning.
Automatic format detection leaves the option disabled and still requires a
recognizable header.

The additional Cisco formats use the existing Cisco field mapping: a prefix
such as `%ASA-6-302014:` becomes `app_name: "ASA-6-302014"`, and `content`
contains the remaining description. Both parsing entry points now support
these Cisco formats. The syslog `severity` continues to come from the PRI
prefix, even if the embedded Cisco severity differs.
