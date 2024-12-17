# erase

Removes entries from a context.

```tql
context::update name:string, key=any
```

## Description

The `context::erase` operator removes data from a context.

Use the `key` argument to specify the field in the input that should be
deleted from the context.

### `name: string`

The name of the context to remove entries from.

### `key = any`

The field that represents the enrichment key in the data.

## See Also

[`context::create_bloom_filter`](create_bloom_filter.md),
[`context::create_geoip`](create_geoip.md),
[`context::create_lookup_table`](create_lookup_table.md),
[`context::enrich`](enrich.md),
[`context::inspect`](inspect.md),
[`context::list`](list.md),
[`context::load`](load.md),
[`context::remove`](remove.md),
[`context::reset`](reset.md),
[`context::save`](save.md),
[`context::update`](update.md)
