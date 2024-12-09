---
sidebar_custom_props:
  operator:
    transformation: true
---

# unique

Removes adjacent duplicates.

## Synopsis

```
unique
```

## Description

The `unique` operator deduplicates adjacent values, similar to the Unix tool
`uniq`.

A frequent use case is [selecting a set of fields](select.md), [sorting the
input](sort.md), and then removing duplicates from the input.

## Examples

Consider the following data:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
{"foo": null, "bar": "b"}
{"foo": null, "bar": "b"}
```

The `unique` operator removes adjacent duplicates and produces the following output:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
{"foo": null, "bar": "b"}
```

Note that the output still contains the event `{"foo": null, "bar": "b"}` twice.
This is because `unique` only removes *adjacent* duplicates.

To remove *all* duplicates (including non-adjacent ones), [`sort`](sort.md)
the input first such that duplicate values lay adjacent to each other. Unlike
deduplication via `unique`, sorting is a blocking and operation and consumes
the entire input before producing outputs.
