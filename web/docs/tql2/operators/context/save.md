# save

Saves context state.

```tql
context::save name:string
```

## Description

The `context::save` operator dumps the state of the specified context into its
(binary) output.

### `name : string`

The name of the context whose state to save.

## Examples

### Store the database of a GeoIP context

```tql
context::save "ctx"
save_file "snapshot.mmdb"
```
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
[`context::update`](update.md)
