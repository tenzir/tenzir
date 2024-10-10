---
sidebar_custom_props:
  operator:
    transformation: true
---

# flatten

Flattens nested data.

## Synopsis

```
flatten [<separator>]
```

## Description

The `flatten` operator acts on [container types](../data-model/type-system.md):

1. **Records**: Join nested records with a separator (`.` by default). For
   example, if a field named `x` is a record with fields `a` and `b`, flattening
   will lift the nested record into the parent scope by creating two new fields
   `x.a` and `x.b`.
2. **Lists**: Merge nested lists into a single (flat) list. For example,
   `[[[2]], [[3, 1]], [[4]]]` becomes `[2, 3, 1, 4]`.

For records inside lists, `flatten` "pushes lists down" into one list per record
field. For example, the record

```json
{
  "foo": [
    {
      "a": 2,
      "b": 1
    },
    {
      "a": 4
    }
  ]
}
```

becomes

```json
{
  "foo.a": [2, 4],
  "foo.b": [1, null]
}
```

Lists nested in records that are nested in lists will also be flattened. For
example, the record

```json
{
  "foo": [
    {
      "a": [
        [2, 23],
        [1,16]
      ],
      "b": [1]
    },
    {
      "a": [[4]]
    }
  ]
}
```

becomes

```json
{
  "foo.a": [
    2,
    23,
    1,
    16,
    4
  ],
  "foo.b": [
    1
  ]
}
```

As you can see from the above examples, flattening also removes `null` values.

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
