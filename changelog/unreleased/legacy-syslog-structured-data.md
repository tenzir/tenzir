---
title: Extract structured data from legacy syslog content
type: feature
authors:
  - mavam
  - codex
pr: 5902
created: 2026-03-13T12:34:47.000000Z
---

`read_syslog` and `parse_syslog` now extract a leading RFC 5424-style
structured-data block from RFC 3164 message content.

This pattern occurs in practice with some VMware ESXi messages, where
components such as `Hostd` emit a legacy syslog record and prepend structured
metadata before the human-readable message text.

For example, this raw syslog line:

```text
<166>2026-02-11T18:01:45.587Z esxi-01.example.invalid Hostd[2099494]: [Originator@6876 sub=Vimsvc.TaskManager opID=11111111-2222-3333-4444-555555555555] Task Completed
```

now parses as:

```tql
{
  facility: 20,
  severity: 6,
  timestamp: "2026-02-11T18:01:45.587Z",
  hostname: "esxi-01.example.invalid",
  app_name: "Hostd",
  process_id: "2099494",
  structured_data: {
    "Originator@6876": {
      sub: "Vimsvc.TaskManager",
      opID: "11111111-2222-3333-4444-555555555555",
    },
  },
  content: "Task Completed",
}
```

Events without extracted structured data keep the existing `syslog.rfc3164`
schema. Events with extracted structured data use
`syslog.rfc3164.structured`.
