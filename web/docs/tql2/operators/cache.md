# cache

:::warning Expert Operator
We designed the `cache` operator for under-the-hood use of the Tenzir Platform
on [app.tenzir.com](https://app.tenzir.com). We generally recommend not using
the operator by yourself, but rather relying on the Tenzir Platform to
automatically manage caches for you.
:::

An in-memory cache shared between pipelines.

```tql
cache id:str, [mode=str, capacity=uint, ttl=duration, max_ttl=duration]
```

## Description

The `cache` operator caches events in an in-memory buffer at a node. Caches must
have a user-provided unique ID.

The first pipeline to use a cache writes into the cache. All further pipelines
using the same cache will read from the cache instead of executing the operators
before the `cache` operator in the same pipeline.

### `id: str`

An arbitrary string that uniquely identifies the cache.

### `mode = str (optional)`

Configures whether the operator is used a source, a sink, or a transformation.
The following modes are available currently:

- `"read"`: The operators acts as a source reading from a cache that is requires to
  already exist.
- `"write"`: The operator acts as a sink writing into a cache that must not
  already exist.
- `"readwrite"`: The operator acts as a transformation passing through events,
  lazily creating a cache if it does not already exist. If a cache exists,
  upstream operators will not be run and instead the cache is read.

Defaults to `"readwrite"`.

### `capacity = uint (optional)`

Stores how many events the cache can hold. Caches stop accepting events if the
capacity is reached and emit a warning.

Defaults to `4Mi`.

### `ttl = duration (optional)`

Defines the maximum inactivity time until the cache is evicted from memory. The
timer starts when writing the cache completes (or runs into the capacity limit),
and resets whenever the cache is read from.

Defaults to `1min`.

### `max_ttl = duration (optional)`

If set, defines an upper bound for the lifetime of the cache. Unlike the `ttl`
option, this does not refresh when the cache is accessed.

## Examples

Cache the results of an expensive query:

```tql
export
where @name == "suricata.flow"
summarize total=sum(bytes_toserver), src_ip, dest_ip
cache "some-unique-identifier"
```

Get some high-level statistics about the query, calculating the cache again only
if it does not exist anymore, deleting the cache if it's unused for more than a
minute.

```tql
export
where @name == "suricata.flow"
summarize src_ip, total=sum(bytes_toserver), dest_ip
cache "some-unique-identifier", ttl=1min
summarize src_ip, total=sum(total), destinations=count(dest_ip)
```

Get the same statistics, assuming the cache still exists:

```tql
cache "some-unique-identifier", mode="read"
summarize src_ip, total=sum(total), destinations=count(dest_ip)
```
