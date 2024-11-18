# reset

Resets a context.

```tql
context::reset name:string
```

## Description

The `context::reset` operator erases all data that has been added with
`context::update`.

### `name : string`

The name of the context to reset.

## Examples

### Reset a context

```tql
context::reset "ctx"
```

## See Also

[`context::create_lookup_table`](create_lookup_table.md),
[`context::create_bloom_filter`](create_bloom_filter.md),
[`context::create_geoip`](create_geoip.md),
[`context::remove`](remove.md),
[`context::update`](update.md),
[`context::inspect`](inspect.md),
[`context::save`](save.md),
[`context::load`](load.md)
