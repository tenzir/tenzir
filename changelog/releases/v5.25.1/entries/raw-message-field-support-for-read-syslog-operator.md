---
title: Raw message field support for read_syslog operator
type: feature
authors:
  - mavam
  - claude
pr: 5687
created: 2026-01-27T14:24:06.018857Z
---

The `read_syslog` operator now supports a `raw_message` parameter that preserves the original, unparsed syslog message in a field of your choice. This is useful when you need to retain the exact input for auditing, debugging, or compliance purposes.

When you specify `raw_message=<field>`, the operator stores the complete input message (including all lines for multiline messages) in the specified field. This works with all syslog formats, including RFC 5424, RFC 3164, and octet-counted messages.

For example:

```tql
read_syslog raw_message=original_input
```

This stores the unparsed message in the `original_input` field alongside the parsed structured fields like `hostname`, `app_name`, `message`, and others.
