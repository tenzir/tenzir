---
title: "Add drop_nulls operator"
type: feature
authors: mavam
pr: 5370
---

The new `drop_nulls` operator removes fields containing null values from
events. Without arguments, it drops all fields with null values. With field
arguments, it drops only the specified fields if they contain null values.

### Drop all null fields

Clean up events by removing all fields with null values:

```tql
from {
  timestamp: 2025-01-23T10:00:00,
  src_ip: 192.168.1.5,
  dst_ip: null,
  username: "alice",
  session_id: null,
  bytes: 1024
}
drop_nulls
```

```tql
{
  timestamp: 2025-01-23T10:00:00Z,
  src_ip: 192.168.1.5,
  username: "alice",
  bytes: 1024,
}
```

### Drop specific null fields

Target specific fields for removal only when they contain null:

```tql
from {
  event_id: 42,
  user: {name: "bob", email: null},
  metadata: null,
  tags: ["security", "audit"]
}
drop_nulls metadata, user.email
```

```tql
{
  event_id: 42,
  user: {
    name: "bob",
  },
  tags: [
    "security",
    "audit",
  ],
}
```

The `metadata` field is removed because it's null at the top level. The
`user.email` field is also removed even though it's nested, showing that
`drop_nulls` can target specific nested fields when explicitly named.
