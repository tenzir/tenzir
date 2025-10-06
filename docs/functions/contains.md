---
title: contains
category: Utility
example: 'this.contains("value")'
---

Searches for a value within data structures recursively.

```tql
contains(input:any, target:any, [exact:bool]) -> bool
```

## Description

The `contains` function returns `true` if the `target` value is found anywhere within the `input` data structure, and `false` otherwise. The search is performed recursively, meaning it will look inside nested records, lists, and other compound data structures.

By default, strings match via substring search and subnets use containment checks. When `exact` is set to `true`, only exact matches are considered.

### `input: any`

The data structure to search within. Can be any type including primitives, records, lists, and nested structures.

### `target: any`

The value to search for. Cannot be a list or record.

### `exact: bool` (optional)

Controls the matching behavior:
- When `false` (default): strings match via substring search, and subnets/IPs use containment checks
- When `true`: only exact equality matches are considered

## Examples

### Search within records

```tql
from {name: "Alice", age: 30, active: true}
found_alice = contains(this, "Alice")
found_bob = contains(this, "Bob")
found_30 = contains(this, 30)
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
found_john = contains(this, "John")
found_theme = contains(user, "dark")
found_missing = contains(this, "light")
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
found_42 = contains(numbers, 42)
found_important = contains(tags, "important")
found_missing = contains(numbers, 99)
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
search_int = contains(values, 42)
search_uint = contains(values, 42.uint())
search_double = contains(values, 42.0)
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
deep_search = contains(data, "found")
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

### Substring search in strings

```tql
from {message: "Hello, World!"}
substring_match = contains(message, "World")
exact_match = contains(message, "World", exact=true)
partial_no_match = contains(message, "Universe")
exact_no_match = contains(message, "Hello, World", exact=true)
```

```tql
{
  message: "Hello, World!",
  substring_match: true,
  exact_match: false,
  partial_no_match: false,
  exact_no_match: false,
}
```

### Subnet and IP containment

```tql
from {subnet: 10.0.0.0/8}
contains_ip = contains(subnet, 10.1.2.3)
contains_subnet = contains(subnet, 10.0.0.0/16)
exact_subnet = contains(subnet, 10.0.0.0/8, exact=true)
```

```tql
{
  subnet: 10.0.0.0/8,
  contains_ip: true,
  contains_subnet: true,
  exact_subnet: true,
}
```

## See Also

[`has`](/reference/functions/has),
[`match_regex`](/reference/functions/match_regex)
