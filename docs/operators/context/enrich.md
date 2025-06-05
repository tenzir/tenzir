---
title: context::enrich
category: Contexts
example: 'context::enrich "ctx", key=x'
---

Resets data with a context.

```tql
context::enrich name:string, key=any,
               [into=field, mode=string, format=string]
```

## Description

The `context::inspect` operator shows details about a specified context.

### `name: string`

The name of the context to inspect.

### `key = any`

The field to use for the context lookup.

### `into = field (optional)`

The field into which to write the enrichment.

Defaults to the context name (`name`).

### `mode = string (optional)`

The mode of the enrichment operation:

- `set`: overwrites the field specified by `into`.
- `append`: appends into the list specified by `into`. If `into` is `null` or an
  `empty` list, a new list is created. If `into` is not a list, the enrichment
  will fail with a warning.

Defaults to `set`.

### `format = string (optional)`

The style of the enriched value:

- `plain`: formats the enrichment as retrieved from the context.
- `ocsf`: formats the enrichment as an [OCSF
  Enrichment](https://schema.ocsf.io/1.4.0-dev/objects/enrichment?extensions=)
  object with fields `data`, `provider`, `type`, and `value`.

Defaults to `plain`.

## Examples

### Enrich with a lookup table

Create a lookup table:

```tql
context::create_lookup_table "ctx"
```

Add data to the lookup table:

```tql
from {x:1, y:"a"},
     {x:2, y:"b"}
context::update "ctx", key=x, value=y
```

Enrich with the table:

```tql
from {x:1}
context::enrich "ctx", key=x
```

```tql
{
  x: 1,
  ctx: "a",
}
```

### Enrich as OCSF Enrichment

Assume the same table preparation as above, but followed by a different call to
`context::enrich` using the `format` option:

```tql
from {x:1}
context::enrich "ctx", key=x, format="ocsf"
```

```tql
{
  x: 1,
  ctx: {
    created_time: 2024-11-18T16:35:48.069981,
    name: "x",
    value: 1,
    data: "a",
  }
}
```

### Enrich by appending to an array

Enrich twice with the same context and accumulate enrichments into an array:

```tql
from {x:1}
context::enrich "ctx", key=x, into=enrichments, mode="append"
context::enrich "ctx", key=x, into=enrichments, mode="append"
```

```tql
{
  x: 1,
  enrichments: [
    "a",
    "a",
  ]
}
```

## See Also

[`context::create_bloom_filter`](/reference/operators/context/create_bloom_filter),
[`context::create_lookup_table`](/reference/operators/context/create_lookup_table),
[`context::load`](/reference/operators/context/load),
[`context::remove`](/reference/operators/context/remove),
[`context::reset`](/reference/operators/context/reset),
[`context::save`](/reference/operators/context/save),
[`create_geoip`](/reference/operators/context/create_geoip),
[`erase`](/reference/operators/context/erase),
[`inspect`](/reference/operators/context/inspect),
[`list`](/reference/operators/context/list),
[`update`](/reference/operators/context/update)
