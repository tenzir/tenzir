---
title: "Dropping null fields"
type: feature
authors: mavam
pr: 5370
---

The new `drop_null_fields` operator removes fields containing null values from
events. Without arguments, it drops all fields with null values. With field
arguments, it drops only the specified fields if they contain null values.

### Drop all null fields

```tql
from {
  id: 42,
  user: {name: "alice", email: null},
  status: null,
  tags: ["security", "audit"]
}
drop_null_fields
```

```tql
{
  id: 42,
  user: {
    name: "alice",
  },
  tags: [
    "security",
    "audit",
  ],
}
```

### Drop specific null fields

```tql
from {
  id: 42,
  user: {name: "alice", email: null},
  status: null,
  tags: ["security", "audit"]
}
drop_null_fields user.email
```

```tql
{
  id: 42,
  user: {
    name: "alice",
  },
  status: null,
  tags: [
    "security",
    "audit",
  ],
}
```

Note that `status` remains because it wasn't specified in the field list.
