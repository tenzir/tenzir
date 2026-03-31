---
title: Spurious warning for Other (99) enum sibling in ocsf::derive
type: bugfix
authors:
  - mavam
  - claude
pr: 5949
created: 2026-03-25T15:44:26.179481Z
---

`ocsf::derive` no longer emits a false warning when an `_id` field is set
to `99` (Other) and the sibling string contains a source-specific value.

Per the OCSF specification, `99`/Other is an explicit escape hatch: the
integer signals that the value is not in the schema's enumeration and the
companion string **must** hold the raw value from the data source. For
example, the following is now accepted silently:

```tql
from {
  metadata: { version: "1.7.0" },
  type_uid: 300201,
  class_uid: 3002,
  auth_protocol_id: 99,
  auth_protocol: "Negotiate",
}
ocsf::derive
```

Previously this produced a spurious `warning: found invalid value for
'auth_protocol'` because `"Negotiate"` is not a named enum caption.
