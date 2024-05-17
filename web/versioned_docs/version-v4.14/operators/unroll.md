---
sidebar_custom_props:
  operator:
    transformation: true
---

# unroll

Unrolls a list by producing multiple events, one for each item.

## Synopsis

```
unroll <field>
```

## Description

The `unroll` operator transforms each input event into a multiple output events.
For each item in the input list, one output event is created, where the list is
replaced with its item. The surrounding data is kept as-is.

![Unroll Example](unroll.excalidraw.svg)

No output events are produced if the list is empty or if the field is `null`.

## Examples

Consider the following events:

```json
{"a": 1, "b": [1, 2, 3]}
{"a": 2, "b": [1]}
{"a": 3, "b": []}
{"a": 4, "b": null}
```

`unroll b` would produce the following output:

```json
{"a": 1, "b": 1}
{"a": 1, "b": 2}
{"a": 1, "b": 3}
{"a": 2, "b": 1}
```

The `unroll` operator can also be used with records.

```json
{
  "src": "192.168.0.5",
  "conn": [
    {
      "dest": "192.168.0.34",
      "active": "381ms"
    },
    {
      "dest": "192.168.0.120",
      "active": "42ms"
    },
    {
      "dest": "1.2.3.4",
      "active": "67ms"
    }
  ]
}
```

We can use `unroll conn` to bring this into a form more suited for analysis.
For example, we would then be able to use
`where active > 100ms || conn.dest !in 192.168.0.0/16` to filter for relevant
connections.

```json
{
  "src": "192.168.0.5",
  "conn": {
    "dest": "192.168.0.34",
    "active": "381.0ms"
  }
}
{
  "src": "192.168.0.5",
  "conn": {
    "dest": "1.2.3.4",
    "active": "67.0ms"
  }
}
```
