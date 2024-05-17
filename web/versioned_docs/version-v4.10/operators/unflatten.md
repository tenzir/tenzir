---
sidebar_custom_props:
  operator:
    transformation: true
---

# unflatten

Unflattens data structures whose field names imply a nested structure.

## Synopsis

```
unflatten [<separator>]
```

## Description

The `unflatten` operator creates nested records out of record entries whose
names include a separator, thus unflattening

:::info
`unflatten` uses a heuristic to determine the unflattened schema. Thus, the
schema of a record that has been flattened using the [`flatten`](flatten.md) operator and
unflattened afterwards may not be identical to the schema of the unmodified
record.
:::

### `<separator>`

The separator string to unflatten records with.

Defaults to `.`.

## Examples

Consider the following data:

```json
{
  "src_ip": "147.32.84.165",
  "src_port": 1141,
  "dest_ip": "147.32.80.9",
  "dest_port": 53,
  "event_type": "dns",
  "dns.type": "query",
  "dns.id": 553,
  "dns.rrname": "irc.freenode.net",
  "dns.rrtype": "A",
  "dns.tx_id": 0,
  "dns.grouped.A": ["tenzir.com"]
}
```

The `unflatten` operator recreates nested records from fields that contain the `.`
separator:

```json
{
  "src_ip": "147.32.84.165",
  "src_port": 1141,
  "dest_ip": "147.32.80.9",
  "dest_port": 53,
  "event_type": "dns",
  "dns": {
    "type": "query",
    "id": 553,
    "rrname": "irc.freenode.net",
    "rrtype": "A",
    "tx_id": 0,
    "grouped": {
      "A": [
        "tenzir.com"
      ]
    }
  }
}
```
