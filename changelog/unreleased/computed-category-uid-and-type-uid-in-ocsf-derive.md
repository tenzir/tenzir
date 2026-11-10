---
title: Computed category_uid and type_uid in ocsf_derive
type: feature
authors:
  - zedoraps
created: 2026-10-28T10:01:27.968233Z
---

The `ocsf_derive` operator and function now compute missing `category_uid` and `type_uid` fields, so OCSF mappings no longer need to set them:

```tql
from {class_uid: 1001, metadata: {version: "1.5.0"}, activity_id: 1}
ocsf_derive
```

```tql
{
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
  activity_id: 1,
  category_uid: 1,
  type_uid: 100101,
  activity_name: "Create",
  category_name: "System Activity",
  class_name: "File System Activity",
  type_name: "File System Activity: Create",
}
```

The `category_uid` follows from the class, including extension classes. The `type_uid` is `class_uid * 100 + activity_id`, with `activity_name` as a fallback when `activity_id` is missing. Existing values stay, and `ocsf_derive` warns when a `type_uid` does not match `class_uid` and `activity_id`.
