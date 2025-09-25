---
title: search
category: Data/Search
example: 'record.search("value")'
---

Searches for a value within data structures recursively.

```tql
search(input:any, target:any) -> bool
```

## Description

The `search` function returns `true` if the `target` value is found anywhere within the `input` data structure, and `false` otherwise. The search is performed recursively, meaning it will look inside nested records, lists, and other compound data structures.

The function performs exact value matching with type compatibility for numeric types (int, uint, double).

### `input: any`

The data structure to search within. Can be any type including primitives, records, lists, and nested structures.

### `target: any`

The value to search for. The search performs exact matching with type-compatible comparisons for numeric types.

## Examples

### Search within records

```tql
from {name: "Alice", age: 30, active: true}
found_alice = search(this, "Alice")
found_bob = search(this, "Bob")
found_30 = search(this, 30)
```

```tql
{
  name: "Alice",
  age: 30,
  active: true,
  found_alice: true,
  found_bob: false,
  found_30: true,
}
```

### Search within nested structures

```tql
from {user: {profile: {name: "John", settings: {theme: "dark"}}}}
found_john = search(this, "John")
found_theme = search(user, "dark")
found_missing = search(this, "light")
```

```tql
{
  user: {
    profile: {
      name: "John",
      settings: {
        theme: "dark",
      },
    },
  },
  found_john: true,
  found_theme: true,
  found_missing: false,
}
```

### Search within lists

```tql
from {numbers: [1, 2, 3, 42], tags: ["important", "urgent"]}
found_42 = search(numbers, 42)
found_important = search(tags, "important")
found_missing = search(numbers, 99)
```

```tql
{
  numbers: [1, 2, 3, 42],
  tags: ["important", "urgent"],
  found_42: true,
  found_important: true,
  found_missing: false,
}
```

### Search with numeric type compatibility

```tql
from {values: {int_val: 42, uint_val: 42.uint(), double_val: 42.0}}
search_int = search(values, 42)
search_uint = search(values, 42.uint())
search_double = search(values, 42.0)
```

```tql
{
  values: {
    int_val: 42,
    uint_val: 42,
    double_val: 42.0,
  },
  search_int: true,
  search_uint: true,
  search_double: true,
}
```

### Search in deeply nested structures

```tql
from {
  data: {
    level1: {
      level2: {
        level3: {
          target: "found"
        }
      }
    }
  }
}
deep_search = search(data, "found")
```

```tql
{
  data: {
    level1: {
      level2: {
        level3: {
          target: "found",
        },
      },
    },
  },
  deep_search: true,
}
```

## See Also

[`has`](/reference/functions/has),
[`match_regex`](/reference/functions/match_regex),
[`contains`](/reference/functions/contains)