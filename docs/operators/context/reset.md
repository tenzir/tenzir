---
title: reset
category: Contexts
example: 'context::reset "ctx"'
---
Resets a context.

```tql
context::reset name:string
```

## Description

The `context::reset` operator erases all data that has been added with
`context::update`.

### `name: string`

The name of the context to reset.

## Examples

### Reset a context

```tql
context::reset "ctx"
```

## See Also

[`context::create_bloom_filter`](/reference/operators/context/create_bloom_filter),
[`context::create_geoip`](/reference/operators/context/create_geoip),
[`context::create_lookup_table`](/reference/operators/context/create_lookup_table),
[`context::load`](/reference/operators/context/load),
[`context::remove`](/reference/operators/context/remove),
[`context::save`](/reference/operators/context/save),
[`enrich`](/reference/operators/context/enrich),
[`erase`](/reference/operators/context/erase),
[`inspect`](/reference/operators/context/inspect),
[`list`](/reference/operators/context/list),
[`update`](/reference/operators/context/update)
