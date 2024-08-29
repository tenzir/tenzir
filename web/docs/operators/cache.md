---
sidebar_custom_props:
  experimental: true
  operator:
    source: true
    transformation: true
    sink: true
---

# cache

An in-memory cache shared between pipelines.

## Synopsis

```
cache [<id>] [--mode <read|write|readwrite>] [--capacity <capacity>]
             [--ttl <duration>] [--max-ttl <duration>]
```

## Description

The `cache` operator caches events in an in-memory buffer at a node. Caches must
have a user-provided unique ID.

The first pipeline to use a cache writes into the cache. All further pipelines
using the same cache will read from the cache instead of executing the operators
before the `cache` operator in the same pipeline.

:::warning Expert Operator
We designed the `cache` operator for under-the-hood use of the Tenzir Platform
on [app.tenzir.com](https://app.tenzir.com). We generally recommend not using
the operator by yourself, but rather relying on the Tenzir Platform to
automatically manage caches for you.
:::

### `<id>`

An arbitrary string that uniquely identifies the cache.

### `--mode <read|write|readwrite>`

Configures whether the operator is used a source, a sink, or a transformation:

- `read`: The operators acts as a source reading from a cache that is requires to
  already exist.
- `write`: The operator acts as a sink writing into a cache that must not
  already exist.
- `readwrite`: The operator acts as a transformation passing through events,
  lazily creating a cache if it does not already exist. If a cache exists,
  upstream operators will not be run.

Defaults to `readwrite`.

### `--capacity <capacity>`

Stores how many events the cache can hold. Caches stop accepting events if the
capacity is reached and emit a warning.

Defaults to 4 Mi.

### `--ttl <duration>`

Defines the maximum inactivity time until the cache is evicted from memory. The
timer starts when writing the cache completes (or runs into the capacity limit),
and resets whenever the cache is read from.

Defaults to 1 minute.

### `--max-ttl <duration>`

If set, defines an upper bound for the lifetime of the cache. Unlike the `--ttl`
option, this does not refresh when the cache is accessed.

Disabled by default.

## Examples

Cache the results of an expensive query:

```
export
| where :ip in 192.168.0.0/16
| cache "my-cache"
```

Get some high-level statistics about the query, calculating the cache again only
if it does not exist anymore:

```
export
| where :ip in 192.168.0.0/16
| cache "my-cache"
| set schema=#schema
| summarize count(.) by #schema
```

Get the same statistics, but do not recompute the cache if it doesn't exist
anymore:

```
cache "my-cache" --mode "read"
| set schema=#schema
| summarize count(.) by #schema
```
