---
title: Renamed builtin operators work on the legacy executor again
type: bugfix
authors:
  - tobim
created: 2026-08-31T08:13:24.591754Z
---

The v6.13.0 rename of module-qualified builtin operators to flat names split
their two implementations across different registry entries: the legacy
implementation stayed under the old spelling, while the flat name only carried
the new-IR implementation. As a result, pipelines running on the legacy
executor—such as deployed pipelines restored by the node on startup—failed
with `this operator can only be used with the new IR` for `context_*`,
`ocsf_cast`, `ocsf_derive`, `ocsf_trim`, `package_*`, and `pipeline_*`
operators, under both the old and the new spelling.

Both halves now register under the flat name again, so these operators work on
both execution paths with either spelling.
