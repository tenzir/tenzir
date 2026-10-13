---
title: OCSF functions
type: feature
authors:
  - raxyte
  - IyeOnline
created: 2026-10-13T16:11:21.076247Z
---

The `ocsf_cast`, `ocsf_trim`, and `ocsf_derive` operators now have function counterparts of the same names. The functions take a record and accept the same options as the operators, so that you can normalize OCSF events that are nested in a field instead of being the top-level event:

```tql
from {source: "edr", ocsf: {class_uid: 1001, metadata: {version: "1.5.0"}, activity_id: 1}}
ocsf = ocsf.ocsf_cast()
```

```tql
{
  source: "edr",
  ocsf: {
    class_uid: 1001,
    metadata: {
      version: "1.5.0",
    },
    activity_id: 1,
  },
}
```

A `null` input yields `null`. For other values that are not records or that lack the OCSF `metadata` field, the functions return `null` with a warning.
