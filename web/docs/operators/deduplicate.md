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

The `deduplicate` operator deduplicates values over a stream of events, based
on the value of one or more fields.

### `<extractor>...`

A comma-separated list of extractors
that identify the fields used for deduplicating.
Defaults to the entire event.

### `--limit <count>`

The number of duplicates allowed before they're removed.
Defaults to 1.

### `--distance <count>`

Distance between two events that can be considered duplicates.
Value of `1` means only adjacent events can be considered duplicates.
`0` means infinity. Defaults to infinity.

### `--timeout <duration>`

The amount of time a specific value is remembered for deduplication.
For each value, the timer is reset every time a match for that value is found.
Defaults to infinity.

## Examples

For the following data:

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

if `deduplicate --limit 1` is used, all duplicate events are removed:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
```

On the other hand, if `deduplicate bar --limit 1` is used,
only the `bar` field is considered
when determining whether an event is a duplicate:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
```
