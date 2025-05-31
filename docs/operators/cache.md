---
title: cache
category: Internals
example: 'cache "w01wyhTZm3", ttl=10min'
---
An in-memory cache shared between pipelines.

```tql
cache id:string, [mode=string, capacity=int, read_timeout=duration, write_timeout=duration]
```

:::note[Used by the Tenzir Platform]
The Tenzir Platform heavily relies on the `cache` operator to make data access
faster and more reliable in both the Explorer and on Dashboards.
:::

## Description

The `cache` operator caches events in an in-memory buffer at a node. Caches must
have a user-provided unique ID.

The first pipeline to use a cache writes into the cache. All further pipelines
using the same cache will read from the cache instead of executing the operators
before the `cache` operator in the same pipeline.

:::tip[Cache Policy]
Set the `tenzir.cache` configuration section controls how caches behave in
Tenzir Nodes:
- `tenzir.cache.lifetime` sets the default write timeout for newly created
  caches.
- `tenzir.cache.capacity` sets an upper bound for the estimated total memory
  usage in bytes across all caches in a node. If the memory usage exceeds this
  limit, the node will start evicting caches to make room for new data. This
  eviction process happens at most once every 30 seconds. The node requires a
  minimum total cache capacity of 64MiB.

```yaml
tenzir:
  cache:
    lifetime: 10min
    capacity: 1Gi
```
:::

### `id: string`

An arbitrary string that uniquely identifies the cache.

### `mode = string (optional)`

Configures whether the operator is used an input, an output, or a transformation.
The following modes are available currently:

- `"read"`: The operators acts as an input operator reading from a cache that is
  requires to already exist.
- `"write"`: The operator acts as an output operator writing into a cache that
  must not already exist.
- `"readwrite"`: The operator acts as a transformation passing through events,
  lazily creating a cache if it does not already exist. If a cache exists,
  upstream operators will not be run and instead the cache is read.

Defaults to `"readwrite"`.

### `capacity = int (optional)`

Stores how many events the cache can hold. Caches stop accepting events if the
capacity is reached and emit a warning.

Defaults to unlimited.

### `read_timeout = duration (optional)`

Defines the maximum inactivity time until the cache is evicted from memory. The
timer starts when writing the cache completes (or runs into the capacity limit),
and resets whenever the cache is read from.

Defaults to `10min`, or the value specified in the `tenzir.cache.lifetme`
option.

### `write_timeout = duration (optional)`

If set, defines an upper bound for the lifetime of the cache. Unlike the
`read_timeout` option, this does not refresh when the cache is accessed.

## Examples

### Cache the results of an expensive query

```tql
export
where @name == "suricata.flow"
summarize total=sum(bytes_toserver), src_ip, dest_ip
cache "some-unique-identifier"
```

### Get high-level statistics about a query

This calculates the cache again only if the query does not exist anymore, and
delete the cache if it's unused for more than a minute.

```tql
export
where @name == "suricata.flow"
summarize src_ip, total=sum(bytes_toserver), dest_ip
cache "some-unique-identifier", read_timeout=1min
summarize src_ip, total=sum(total), destinations=count(dest_ip)
```

Get the same statistics, assuming the cache still exists:

```tql
cache "some-unique-identifier", mode="read"
summarize src_ip, total=sum(total), destinations=count(dest_ip)
```

## See Also

[`buffer`](/reference/operators/buffer)
