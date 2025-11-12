---
title: ocsf::derive
category: OCSF
example: 'ocsf::derive'
---

Automatically assigns enum strings from their integer counterparts and vice
versa.

```tql
ocsf::derive
```

## Description

The `ocsf::derive` operator performs bidirectional enum derivation for OCSF
events by automatically assigning enum string values based on their integer
counterparts and vice versa.

In the future, this operator will also assign automatically assign computable
values such as `type_uid` based on what is available in the event.

## Examples

### Integer to string

```tql
from {
  activity_id: 1,
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
}
ocsf::derive
```
```tql
{
  activity_id: 1,
  activity_name: "Create",
  class_name: "File System Activity",
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
}
```

### String to Integer

```tql
from {
  activity_name: "Read",
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
}
ocsf::derive
```
```tql
{
  activity_id: 2,
  activity_name: "Read",
  class_name: "File System Activity",
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
}
```

### Bidirectional enum validation

```tql
from {
  activity_id: 1,
  activity_name: "Delete",  // Inconsistent with activity_id=1
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
}
ocsf::derive
```
```tql
{
  activity_id: 1,
  activity_name: "Delete",
  class_name: "File System Activity",
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
}
```

This will emit a warning about inconsistent values and preserve both original
values without modification, allowing the user to decide how to handle the
conflict.

```
warning: found inconsistency between `activity_id` and `activity_name`
 --> <input>:9:1
  |
9 | ocsf::derive
  | ~~~~~~~~~~~~
  |
  = note: got 1 ("Create") and "Delete" (4)
```

## See Also

[`ocsf::cast`](/reference/operators/ocsf/cast),
[`ocsf::trim`](/reference/operators/ocsf/trim)
