---
title: unordered
category: Internals
example: 'unordered { read_ndjson }'
---

Removes ordering assumptions from a pipeline.

```tql
unordered { … }
```

## Description

The `unordered` operator takes a pipeline as an argument and removes ordering
assumptions from it. This causes some operators to run faster.

Note that some operators implicitly remove ordering assumptions. For example,
`sort` tells upstream operators that ordering does not matter.

## Examples

### Parse JSON unordered

```tql
unordered {
  read_json
}
```
