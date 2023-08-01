---
sidebar_position: 5
---

# Show available schemas

When you write a pipeline, you often reference field names. If you do not know
the shape of your data, you can look up available
[schemas](../data-model/schemas.md), i.e., the record types describing a table.

Many SQL databases of a `SHOW TABLES` to show all available tables. Similarly,
the `show tables` displays a table of a all available table schemas:

```
show tables
```

<details>
<summary>Output</summary>

```json
{
  "name": "zeek.dns",
  "structure": {
    "basic": null,
    "enum": null,
    "list": null,
    "map": null,
    "record": [
      {
        "name": "ts",
        "type": "timestamp"
      },
      {
        "name": "uid",
        "type": "string #index=hash"
      },
      {
        "name": "id",
        "type": "zeek.conn_id"
      },
      {
        "name": "proto",
        "type": "string"
      },
      {
        "name": "trans_id",
        "type": "uint64"
      },
      {
        "name": "rtt",
        "type": "duration"
      },
      {
        "name": "query",
        "type": "string"
      },
      {
        "name": "qclass",
        "type": "uint64"
      },
      {
        "name": "qclass_name",
        "type": "string"
      },
      {
        "name": "qtype",
        "type": "uint64"
      },
      {
        "name": "qtype_name",
        "type": "string"
      },
      {
        "name": "rcode",
        "type": "uint64"
      },
      {
        "name": "rcode_name",
        "type": "string"
      },
      {
        "name": "AA",
        "type": "bool"
      },
      {
        "name": "TC",
        "type": "bool"
      },
      {
        "name": "RD",
        "type": "bool"
      },
      {
        "name": "RA",
        "type": "bool"
      },
      {
        "name": "Z",
        "type": "uint64"
      },
      {
        "name": "answers",
        "type": "list<string>"
      },
      {
        "name": "TTLs",
        "type": "list<duration>"
      },
      {
        "name": "rejected",
        "type": "bool"
      },
      {
        "name": "_write_ts",
        "type": "time"
      }
    ]
  },
  "attributes": null
}
```

(Only one shown.)

</details>

This example shows the schema for a Zeek dns.log. You can see the various
fields as list of key-value pairs under the `record` key. The nested record `id`
that is a type alias with the type name `zeek.conn_id`.
