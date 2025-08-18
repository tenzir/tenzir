---
title: "More lenient RFC 3164 Syslog parsing"
type: feature
authors: IyeOnline
pr: 5426
---

Our syslog parser now allows for a `.` character in the tag/app_name
field and any character in the `process_id` field.
This allows you to parse the log:
```
<21>Aug 18 12:00:00 hostname_redacted .NetRuntime[-]: content...
```
```tql
{
  facility: 2,
  severity: 5,
  timestamp: "Aug 18 12:00:00",
  hostname: "hostname_redacted",
  app_name: ".NetRuntime",
  process_id: "-",
  content: "content...",
}
```
