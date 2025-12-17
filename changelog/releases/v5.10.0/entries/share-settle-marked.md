---
title: "`ocsf::trim` and `ocsf::derive`"
type: feature
author: jachris
created: 2025-07-14T13:33:23Z
pr: 5330
---

Tenzir now provides two new operators for processing OCSF events:

**`ocsf::derive`** automatically assigns enum strings from their integer
counterparts and vice versa. It performs bidirectional enum derivation for OCSF
events and validates consistency between existing enum values.

```tql
from {
  activity_id: 1,
  class_uid: 1001,
  metadata: {version: "1.5.0"},
}
ocsf::derive
```

This transforms the event to include the derived `activity_name: "Create"` and
`class_name: "File System Activity"` fields.

**`ocsf::trim`** intelligently removes fields from OCSF events to reduce data
size while preserving essential information. You can also have explicit control
over optional and recommended field removal.

```tql
from {
  class_uid: 3002,
  class_name: "Authentication",
  user: {
    name: "alice",
    display_name: "Alice",
  },
  status: "Success",
}
ocsf::trim
```

This removes non-essential fields like `class_name` and `user.display_name`
while keeping critical information intact.
