# inspect

Resets a context.

```tql
context::inspect name:string
```

## Description

The `context::inspect` operator shows details about a specified context.

### `name : string`

The name of the context to inspect.

## Examples

### Inspect a context

Add data to the lookup table:

```tql
from [
  {x:1, y:"a"},
  {x:2, y:"b"},
]
context::update "ctx", key=x, value=y
```

Retrieve the lookup table contents:

```tql
context::inspect "ctx"
```

```tql
{key: 2, value: "b"}
{key: 1, value: "a"}
```

## See Also

[`context::create_bloom_filter`](create_bloom_filter.md),
[`context::create_geoip`](create_geoip.md),
[`context::create_lookup_table`](create_lookup_table.md),
[`context::enrich`](enrich.md),
[`context::load`](load.md),
[`context::remove`](remove.md),
[`context::reset`](reset.md),
[`context::save`](save.md),
[`context::update`](update.md)
