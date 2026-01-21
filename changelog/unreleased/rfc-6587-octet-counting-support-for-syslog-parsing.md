---
title: RFC 6587 octet-counting support for syslog parsing
type: feature
authors:
  - mavam
  - claude
created: 2026-01-21T07:41:37.394369Z
---

The `parse_syslog` function now supports RFC 6587 octet-counted framing, where syslog messages are prefixed with their byte length (for example, `65 <syslog-message>`). This framing is commonly used in TCP-based syslog transport to handle message boundaries.

The new `octet_counting` parameter for `parse_syslog` offers three modes:

- **Not specified (default)**: Auto-detect. The parser strips a length prefix if present and valid, otherwise parses the input as-is. This prevents false positives where input coincidentally starts with digits and a space.
- **`octet_counting=true`**: Require a length prefix. Emits a warning and returns null if the input lacks a valid prefix.
- **`octet_counting=false`**: Never strip a length prefix. Parse the input as-is.
