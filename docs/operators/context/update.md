---
title: update
---

Updates a context with new data.

```tql
context::update name:string, key=any,
               [value=any, create_timeout=duration,
                write_timeout=duration, read_timeout=duration]
```

## Description

The `context::update` operator adds new data to a specified context.

Use the `key` argument to specify the field in the input that should be
associated with the context. The [`context::enrich`](/reference/operators/context/enrich) operator uses
this key to access the context. For contexts that support assigning a value with
a given key, you can provide an expression to customize what's being associated
with the given key.

The three arguments `create_timeout`, `write_timeout`, and `read_timeout` only
work with lookup tables and set the respective timeouts per table entry.

### `name: string`

The name of the context to update.

### `key = any`

The field that represents the enrichment key in the data.

### `value = any (optional)`

The field that represents the enrichment value to associate with `key`.

Defaults to `this`.

### `create_timeout = duration (optional)`

Expires a context entry after a given duration since entry creation.

### `write_timeout = duration (optional)`

Expires a context entry after a given duration since the last update time. Every
Every call to `context::update` resets the timeout for the respective key.

### `read_timeout = duration (optional)`

Expires a context entry after a given duration since the last access time.
Every call to `context::enrich` resets the timeout for the respective key.

## Examples

### Populate a lookup table with data

Create a lookup table:

```tql
context::create_lookup_table "ctx"
```

Add data to the lookup table via `context::update`:

```tql
from {x:1, y:"a"},
     {x:2, y:"b"}
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

### Use a custom value as lookup table

```tql
from {x:1},
     {x:2}
context::update "ctx", key=x, value=x*x
```

```tql
{key: 2, value: 4}
{key: 1, value: 1}
```

## See Also

[`context::create_bloom_filter`](/reference/operators/context/create_bloom_filter),
[`context::create_lookup_table`](/reference/operators/context/create_lookup_table),
[`context::load`](/reference/operators/context/load),
[`context::remove`](/reference/operators/context/remove),
[`context::reset`](/reference/operators/context/reset),
[`context::save`](/reference/operators/context/save),
[`create_geoip`](/reference/operators/context/create_geoip),
[`enrich`](/reference/operators/context/enrich),
[`erase`](/reference/operators/context/erase),
[`inspect`](/reference/operators/context/inspect),
[`list`](/reference/operators/context/list)
