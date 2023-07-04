# flatten

Flattens nested data structures.

## Synopsis

```
flatten [<separator>]
```

## Description

The `flatten` operator removes any nested lists or records by merging lists and
joining nested records with a separator. Flattening removes null values.

:::info
Unlike for most data models, flattening is an (almost) free operation in VAST's
data model.
:::

### `<separator>`

The separator string to join nested records with.

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
  "dns": {
    "type": "query",
    "id": 553,
    "rrname": "irc.freenode.net",
    "rrtype": "A",
    "tx_id": 0,
    "grouped": {
      "A": ["tenzir.com", null]
    }
  }
}
```

The `flatten` operator removes any nesting from the data:

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
