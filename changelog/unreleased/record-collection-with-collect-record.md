---
title: Record construction from key-value entries
type: feature
authors:
  - mavam
created: 2026-09-22T08:03:20.036994Z
---

The new `collect_record` function builds a record from key/value entries, two-element pairs, record fragments, or separate lists of keys and values. It preserves mixed value types:

```tql
from {
  entries: [
    ["count", 42],
    ["active", true],
    {source: "sensor"},
  ],
}
select result=collect_record(entries)
```

```tql
{
  result: {
    count: 42,
    active: true,
    source: "sensor",
  },
}
```

Records with exactly `key` and `value` are decoded as entries, like `from_entries` in other languages. Other records contribute their fields directly. Keys are automatically converted to strings, so `42` and `"42"` refer to the same field, and a null key becomes `"null"`. For duplicate keys, the last value wins, including null values. Two-list calls require matching lengths.
