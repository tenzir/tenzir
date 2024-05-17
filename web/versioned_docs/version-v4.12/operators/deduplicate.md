---
sidebar_custom_props:
  operator:
    transformation: true
---

# deduplicate

Removes duplicate events based on the values of one or more fields.

## Synopsis

```
deduplicate [<extractor>...]
            [--limit <count>] [--distance <count>] [--timeout <duration>]
```

## Description

The `deduplicate` operator removes duplicates from a stream of events, based
on the value of one or more fields.

You have three independent configuration options to customize the operator's
behavior:

1. **Limit**: the multiplicity of the events until they are supressed as
   duplicates. A limit of 1 is equivalent to emission of unique events. A limit
   of *N* means that events with a unique key (defined by the fields) get
   emitted at most *N* times. For example, `GGGYBYYBGYGB` with a limit of 2
   yields `GGYBYB`.
2. **Distance**: The number of events in sequence since the last occurrence of
   a unique event. For example, deduplicating a stream `GGGYBYYBGYGB` with
   distance 2 yields `GYBBGYB`.
3. **Timeout**: The time that needs to pass until a surpressed event is no
   longer considered a duplicate. When an event with surpressed key is seen
   before the timeout is reached, the timer resets.

The diagram below illustrates these three options. The different colored boxes
refer to events of different schemas.

![Deduplicate Configuration Knobs](deduplicate.excalidraw.svg)

### `<extractor>...`

A comma-separated list of extractors that identify the fields used for
deduplicating. Missing fields are treated as if they had the value `null`.

Defaults to the entire event.

### `--limit <count>`

The number of duplicates allowed before they are removed.

Defaults to 1.

### `--distance <count>`

Distance between two events that can be considered duplicates. Value of `1`
means only adjacent events can be considered duplicates. `0` means infinity.

Defaults to infinity.

### `--timeout <duration>`

The amount of time a specific value is remembered for deduplication. For each
value, the timer is reset every time a match for that value is found.

Defaults to infinity.

## Examples

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

For `deduplicate --limit 1`, all duplicate events are removed:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
```

If `deduplicate bar --limit 1` is used, only the field `bar` is considered when
determining whether an event is a duplicate:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
```

And for `deduplicate foo --limit 1`, only the field `foo` is considered.
Note, how the missing `foo` field is treated as if it had the value `null`,
i.e., it's not included in the output.

```json
{"foo": 1, "bar": "a"}
{"foo": null, "bar": "b"}
```
