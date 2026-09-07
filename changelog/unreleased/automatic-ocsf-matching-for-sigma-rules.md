---
title: Automatic OCSF matching for Sigma rules
type: feature
authors:
  - mavam
created: 2026-08-30T11:13:12.257999Z
---

Stock Sigma rules now match conformant OCSF events automatically. The `sigma`
operator recognizes OCSF-shaped table schemas, plans matching once per schema,
and preserves source-field semantics such as process identity, Windows users,
registry values, DNS answers, integrity levels, and structured hashes. A
built-in mapping catalog covers the major SigmaHQ logsources: every
Sysmon-backed Windows category, Linux and macOS process creation, the Windows
Security, System, PowerShell, and Defender channels, Okta, Entra ID, Azure
activity logs, AWS CloudTrail, GCP Cloud Audit Logs, and Zeek DNS, HTTP, and
SMB. Four out of five rules in the SigmaHQ corpus translate completely; rules
over source fields that OCSF does not preserve are skipped with a diagnostic
that names the field:

```tql
subscribe "ocsf"
sigma path="sigma/rules"
```

An OCSF-shaped schema has a string `metadata.version` and an `int64` or
`uint64` `class_uid`. Other schemas keep direct field matching. Unknown fields
fall back to literal schema fields, while rules with unresolved fields are
skipped safely. Use `mapping="direct"` to opt out of structural recognition and
match every rule field literally. Output formats and original source evidence
remain unchanged.
