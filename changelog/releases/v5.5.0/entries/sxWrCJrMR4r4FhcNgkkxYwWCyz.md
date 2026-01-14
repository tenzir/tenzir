---
title: "Dedicated OCSF operator"
type: feature
author: jachris
created: 2025-06-18T08:44:27Z
pr: 5220
---

The new operator `ocsf::apply` converts events to the OCSF schema, making sure
that all events have the same type. It supports all OCSF versions (including
`-dev` versions), all OCSF classes and all OCSF profiles. The schema to use is
determined by `class_uid`, `metadata.version` and `metadata.profiles` (if it
exists). The operator emits warnings if it finds unexpected fields or mismatched
types. Expect more OCSF-native functionality coming to Tenzir soon!
