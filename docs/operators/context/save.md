---
title: save
category: Contexts
example: 'context::save "ctx"'
---

Saves context state.

```tql
context::save name:string
```

## Description

The `context::save` operator dumps the state of the specified context into its
(binary) output.

### `name: string`

The name of the context whose state to save.

## Examples

### Store the database of a GeoIP context

```tql
context::save "ctx"
save_file "snapshot.mmdb"
```

## See Also

[`context::create_bloom_filter`](/reference/operators/context/create_bloom_filter),
[`context::create_lookup_table`](/reference/operators/context/create_lookup_table),
[`context::load`](/reference/operators/context/load),
[`context::remove`](/reference/operators/context/remove),
[`context::reset`](/reference/operators/context/reset),
[`create_geoip`](/reference/operators/context/create_geoip),
[`enrich`](/reference/operators/context/enrich),
[`erase`](/reference/operators/context/erase),
[`inspect`](/reference/operators/context/inspect),
[`list`](/reference/operators/context/list),
[`update`](/reference/operators/context/update)
