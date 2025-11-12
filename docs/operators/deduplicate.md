---
title: deduplicate
category: Filter
example: "deduplicate src_ip"
---

Removes duplicate events based on a common key.

```tql
deduplicate [keys:any, limit=int, distance=int, create_timeout=duration,
             write_timeout=duration, read_timeout=duration]
```

## Description

The `deduplicate` operator removes duplicates from a stream of events, based
on the value of one or more fields.

### `keys: any (optional)`

The expressions that form the deduplication key. Pass one or more positional
arguments, for example `deduplicate src_ip, dst_ip`, to build a compound key
from multiple fields. Record expressions, such as `{foo: bar, baz: qux}`,
to work as well.

Defaults to `this`, i.e., deduplicating entire events.

### `limit = int (optional)`

The number of duplicate keys allowed before an event is suppressed.

Defaults to `1`, which is equivalent to removing all duplicates.

### `distance = int (optional)`

Distance between two events that can be considered duplicates. A value of `1`
means that only adjacent events can be considered duplicate.

When unspecified, the distance is infinite.

### `create_timeout = duration (optional)`

The time that needs to pass until a surpressed event is no longer considered a
duplicate. The timeout resets when the first event for a given key is let
through.

### `write_timeout = duration (optional)`

The time that needs to pass until a suppressed event is no longer considered a
duplicate. The timeout resets when any event for a given key is let through.

For a limit of `1`, the write timeout is equivalent to the create timeout.

The write timeout must be smaller than the create timeout.

### `read_timeout = duration (optional)`

The time that needs to pass until a suppressed event is no longer considered a
duplicate. The timeout resets when a key is seen, even if the event is
suppressed.

The read timeout must be smaller than the write and create timeouts.

## Examples

### Deduplicate entire events

Deduplicate a stream of events by using `deduplicate` without arguments:

```tql
from \
  {foo: 1, bar: "a"},
  {foo: 1, bar: "a"},
  {foo: 1, bar: "a"},
  {foo: 1, bar: "b"},
  {foo: null, bar: "b"},
  {bar: "b"},
  {foo: null, bar: "b"},
  {foo: null, bar: "b"}
deduplicate
```

```tql
{foo: 1, bar: "a"}
{foo: 1, bar: "b"}
{foo: null, bar: "b"}
{bar: "b"}
```

### Deduplicate events based on single fields

Use `deduplicate bar` to restrict the deduplication to the values of field
`bar`:

```tql
from \
  {foo: 1, bar: "a"},
  {foo: 1, bar: "a"},
  {foo: 1, bar: "a"},
  {foo: 1, bar: "b"},
  {foo: null, bar: "b"},
  {bar: "b"},
  {foo: null, bar: "b"},
  {foo: null, bar: "b"}
deduplicate bar
```

```tql
{foo: 1, bar: "a"}
{foo: 1, bar: "b"}
```

When writing `deduplicate foo`, note how the missing `foo` field is treated as
if it had the value `null`, i.e., it's not included in the output.

```tql
from \
  {foo: 1, bar: "a"},
  {foo: 1, bar: "a"},
  {foo: 1, bar: "a"},
  {foo: 1, bar: "b"},
  {foo: null, bar: "b"},
  {bar: "b"},
  {foo: null, bar: "b"},
  {foo: null, bar: "b"}
deduplicate foo?
```

```tql
{foo: 1, bar: "a"}
{foo: null, bar: "b"}
```

### Deduplicate events based on multiple fields

Multiple positional arguments form a tuple that must match entirely to suppress
an event. For example, `deduplicate foo, bar` keeps the first event for each
unique combination of `foo` and `bar`:

```tql
from \
  {foo: 1, bar: "a", idx: 1},
  {foo: 1, bar: "a", idx: 2},
  {foo: 1, bar: "b", idx: 3},
  {foo: 2, bar: "a", idx: 4},
  {foo: 1, bar: "b", idx: 5}
deduplicate foo, bar
```

```tql
{foo: 1, bar: "a", idx: 1}
{foo: 1, bar: "b", idx: 3}
{foo: 2, bar: "a", idx: 4}
```

### Get up to 10 warnings per hour for each run of a pipeline

```tql
diagnostics live=true
deduplicate pipeline_id, run, limit=10, create_timeout=1h
```

### Get an event whenever the node disconnected from the Tenzir Platform

```tql
metrics "platform", live=true
deduplicate connected, distance=1
where not connected
```

## See Also

[`sample`](/reference/operators/sample)
