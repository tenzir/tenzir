# enrich

Resets data with a context.

```tql
context::enrich name:string, key=expression,
               [into=field, mode=string, format=string]
```

## Description

The `context::inspect` operator shows details about a specified context.

### `name : string`

The name of the context to inspect.

## Examples

### Enrich with a lookup table

Create a lookup table:

```tql
context::create_lookup_table "ctx"
```

Add data to the lookup table:

```tql
from [
  {x:1, y:"a"},
  {x:2, y:"b"},
]
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

[`context::create_bloom_filter`](create_bloom_filter.md),
[`context::create_geoip`](create_geoip.md),
[`context::create_lookup_table`](create_lookup_table.md),
[`context::inspect`](inspect.md),
[`context::load`](load.md),
[`context::remove`](remove.md),
[`context::reset`](reset.md),
[`context::save`](save.md),
[`context::update`](update.md)
