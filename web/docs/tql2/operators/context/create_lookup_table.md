# create_lookup_table

Creates a lookup table context.

```tql
context::create_lookup_table name:string
```

## Description

The `context::create_lookup_table` operator constructs a new context of type
[lookup table](../../../enrichment/README.md#lookup-table).

### `name : string`

The name of the new lookup table.

## Examples

### Create a new lookup table context

```tql
context::create_lookup_table "ctx"
```

## See Also

[`context::create_bloom_filter`](create_bloom_filter.md),
[`context::create_geoip`](create_geoip.md),
[`context::enrich`](enrich.md),
[`context::inspect`](inspect.md),
[`context::load`](load.md),
[`context::remove`](remove.md),
[`context::reset`](update.md),
[`context::save`](save.md),
[`context::update`](update.md),
