---
title: "Dropping null fields"
type: feature
authors: mavam
pr: 5370
---

The new `drop_null_fields` operator removes fields containing null values from
events. Without arguments, it drops all fields with null values. With field
arguments, it drops the specified fields if they contain null values, and for
record fields, it also recursively drops all null fields within them.

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

### Drop null fields recursively within records

When specifying a record field, all null fields within it are removed:

```tql
from {
  user: {name: "alice", email: null, role: null},
  settings: {theme: "dark", notifications: null}
}
drop_null_fields user
```

```tql
{
  user: {
    name: "alice",
  },
  settings: {
    theme: "dark",
    notifications: null,
  },
}
```

The `user.email` and `user.role` fields are removed because they are null fields
within the specified `user` record. The `settings.notifications` field remains
because `settings` was not specified.
