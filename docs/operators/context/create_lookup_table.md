---
title: create_lookup_table
category: Contexts
example: 'context::create_lookup_table "ctx"'
---

Creates a lookup table context.

```tql
context::create_lookup_table name:string
```

## Description

The `context::create_lookup_table` operator constructs a new context of type
[lookup table](/explanations/enrichment#lookup-table).

You can also create a lookup table as code by adding it to `tenzir.contexts` in
your `tenzir.yaml`:

```yaml title="<prefix>/etc/tenzir/tenzir.yaml"
tenzir:
  contexts:
    my-table:
      type: lookup-table
```

### `name: string`

The name of the new lookup table.

## Examples

### Create a new lookup table context

```tql
context::create_lookup_table "ctx"
```

## See Also

[`context::create_bloom_filter`](/reference/operators/context/create_bloom_filter),
[`context::inspect`](/reference/operators/context/inspect),
[`context::load`](/reference/operators/context/load),
[`context::remove`](/reference/operators/context/remove),
[`context::save`](/reference/operators/context/save),
[`create_geoip`](/reference/operators/context/create_geoip),
[`enrich`](/reference/operators/context/enrich),
[`erase`](/reference/operators/context/erase),
[`list`](/reference/operators/context/list),
[`reset`](/reference/operators/context/reset),
[`update`](/reference/operators/context/update)
