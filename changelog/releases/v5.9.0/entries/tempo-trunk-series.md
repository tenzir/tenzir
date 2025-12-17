---
title: "Add `uuid()` function for generating UUIDs"
type: feature
author: mavam
created: 2025-07-05T07:54:16Z
pr: 5097
---

Need a unique identifier? Look no further! The new `uuid()` function brings the
power of Universally Unique Identifiers to Tenzir, supporting multiple UUID
versions for different use cases.

**Generate tracking IDs for security events:**

```tql
from {
  event_id: uuid(),
  timestamp: now(),
  action: "login_attempt"
}
```

```tql
{
  event_id: "62c9b810-1ecc-4511-9707-977b72c2a9dc",
  timestamp: 2025-07-04T13:47:15.473012Z,
  action: "login_attempt",
}
```

**Create time-ordered database keys with v7:**

```tql
// v7 UUIDs are perfect for database primary keys - they're time-sortable!
from {
  id: uuid(version="v7"),
  created_at: now(),
  user: "alice"
}
```

```tql
{
  id: "0197d5b1-1dc1-7070-804f-d6d749f15f56",
  created_at: 2025-07-04T13:47:23.969114Z,
  user: "alice",
}
```

**Build distributed system identifiers with v1:**

```tql
// v1 includes MAC address for true uniqueness across nodes
from {
  node_id: uuid(version="v1"),
  cluster: "production"
}
```

```tql
{
  node_id: "6eac5cce-58dd-11f0-a47d-33e666d9ff94",
  cluster: "production",
}
```

**Generate secure random tokens with v4 (default):**

```tql
// Perfect for session tokens or API keys
from {
  session_token: uuid(),  // defaults to v4
  expires_at: now() + 1h
}
```

```tql
{
  session_token: "f43e6460-23e2-45a3-87af-f8b7d10c4e35",
  expires_at: 2025-07-04T14:47:44.632335Z,
}
```

**Use v6 for better database performance:**

```tql
// v6 reorders v1 fields for improved database index locality
from {
  record_id: uuid(version="v6"),
  data: "important stuff"
}
```

```tql
{
  record_id: "1f058dd7-aa81-6496-a3ef-bd8da76352a4",
  data: "important stuff",
}
```

**Even generate the special nil UUID:**

```tql
// Sometimes you need all zeros
from {
  placeholder: uuid(version="nil")
}
```

```tql
{
  placeholder: "00000000-0000-0000-0000-000000000000",
}
```

The function supports UUID versions 1, 4 (default), 6, 7, and nil—covering
everything from time-based identifiers to cryptographically secure random IDs.
Whether you're tracking security events, building distributed systems, or just
need a unique identifier, `uuid()` has you covered!
