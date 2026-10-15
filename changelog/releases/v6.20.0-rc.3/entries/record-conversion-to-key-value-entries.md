---
title: Record conversion to key-value entries
type: feature
authors:
  - mavam
created: 2026-09-30T15:47:21.948783Z
---

The new `entries` function converts a record into a list of `{key, value}` entries, with one entry for every top-level field in field order:

```tql
from {
  user: {
    name: "alice",
    age: 42,
  },
}
select entries=user.entries()
```

```tql
{
  entries: [
    {
      key: "name",
      value: "alice",
    },
    {
      key: "age",
      value: 42,
    },
  ],
}
```

Each key is the field name as a string, and a dot in a field name stays part of the key. Each value keeps its original type, including `null`, lists, and records. An empty record produces `[]`, and a null input produces `null`.

The function is the inverse of `collect_record`, so `x.entries().collect_record()` equals `x`. It is called `to_entries` in jq and `Object.entries` in JavaScript.
