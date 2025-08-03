---
title: "Automatic integer casting in `ocsf::apply`"
type: change
authors: jachris
pr: 5401
---

The `ocsf::apply` operator now automatically casts `uint64` values to `int64`
when the OCSF schema expects an integer field. This is important because the
exact integer type is mostly considered an implementation detail. Unsigned
integers are mainly produced when reading events for which a schema has been
defined. This change makes sure that OCSF mappings that use the resulting events
can successfully pass through `ocsf::apply`.

**Example:**

```tql
from {
  class_uid: 4001,
  metadata: {
    version: "1.5.0"
  },
  severity_id: uint(3)
}
ocsf::apply
```

Previously, this would result in a type mismatch warning and the `severity_id`
field would be set to null. Now the `uint64` value 3 is automatically cast to
`int64`, preserving the data. Values that exceed the maximum `int64` value will
still generate a warning and be set to null.
