---
title: deduplicate
category: Filter
example: 'deduplicate src_ip'
---

Removes duplicate events based on a common key.

```tql
deduplicate [key:any, limit=int, distance=int, create_timeout=duration,
             write_timeout=duration, read_timeout=duration]
```

## Description

The `deduplicate` operator removes duplicates from a stream of events, based
on the value of one or more fields.

### `key: any (optional)`

The key to deduplicate. To deduplicate multiple fields, use a record expression
like `{foo: bar, baz: qux}`.

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

### Simple deduplication

Consider the following data:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
{"foo": null, "bar": "b"}
{"foo": null, "bar": "b"}
```

For `deduplicate`, all duplicate events are removed:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
```

If `deduplicate bar` is used, only the field `bar` is considered when
determining whether an event is a duplicate:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
```

And for `deduplicate foo`, only the field `foo` is considered. Note, how the
missing `foo` field is treated as if it had the value `null`, i.e., it's not
included in the output.

```json
{"foo": 1, "bar": "a"}
{"foo": null, "bar": "b"}
```

### Get up to 10 warnings per hour for each run of a pipeline

```tql
diagnostics live=true
deduplicate {id: pipeline_id, run: run}, limit=10, create_timeout=1h
```

### Get an event whenever the node disconnected from the Tenzir Platform

```tql
metrics "platform", live=true
deduplicate connected, distance=1
where not connected
```

## See Also

[`sample`](/reference/operators/sample)
