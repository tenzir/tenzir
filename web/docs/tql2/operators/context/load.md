# load

Loads context state.

```tql
context::load name:string
```

## Description

The `context::load` operator replaces the state of the specified context with
its (binary) input.

### `name : string`

The name of the context whose state to update.

## Examples

### Replace the database of a GeoIP context

```tql
load_file "ultra-high-res.mmdb", mmap=true
context::load "ctx"
```
## See Also

[`context::create_bloom_filter`](create_bloom_filter.md),
[`context::create_geoip`](create_geoip.md),
[`context::create_lookup_table`](create_lookup_table.md),
[`context::enrich`](enrich.md),
[`context::inspect`](inspect.md),
[`context::list`](list.md),
[`context::remove`](remove.md),
[`context::reset`](reset.md),
[`context::save`](save.md),
[`context::update`](update.md)
