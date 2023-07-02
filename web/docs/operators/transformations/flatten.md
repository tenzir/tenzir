# flatten

Flattens nested data.

## Synopsis

```
flatten [<separator>]
```

## Description

The `flatten` operator acts on [container
types](../../data-model/type-system.md):

1. **Records**: join nested records with a separator. For example, if a field
   named `x` is a nested records with fields `a` and `b`, flattening will lift
   the nested records into the parent scope by creating two new fields `x.a` and
   `x.b`.
2. **Lists**: merge nested lists into a single (flat) list. For example,
   `GENERIC-EXAMPLE-MISSING`.

Flattening removes `null` values.

### `<separator>`

The separator string to join the field names of nested records.

Defaults to `.`.

## Examples

Consider the following record:

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

After `flatten` the record looks as follows:

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

 Note that `dns.grouped.A` no longer contains a `null` value.
