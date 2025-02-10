---
sidebar_position: 5
---

# Show available schemas

When you write a pipeline, you often reference field names. If you do not know
the shape of your data, you can look up available schemas, i.e., the record
types describing top-level events.

Many SQL databases have a `SHOW TABLES` command to show all available table
names, and `SHOW COLUMNS` to display the individual fiels of a given table.

In Tenzir, the [`fields`](../tql2/operators/fields.md) operator offers the
ability for detailed schema introspection. Use it to display all schema fields;
each event represents a single field.

```tql
fields
head
```

```json
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "ts", "path": ["ts"], "index": [0], "type": {"kind": "time", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "uid", "path": ["uid"], "index": [1], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": [{"key": "index", "value": "hash"}]}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "orig_h", "path": ["id", "orig_h"], "index": [2, 0], "type": {"kind": "ip", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "orig_p", "path": ["id", "orig_p"], "index": [2, 1], "type": {"kind": "uint64", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "resp_h", "path": ["id", "resp_h"], "index": [2, 2], "type": {"kind": "ip", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "resp_p", "path": ["id", "resp_p"], "index": [2, 3], "type": {"kind": "uint64", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "proto", "path": ["proto"], "index": [3], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "trans_id", "path": ["trans_id"], "index": [4], "type": {"kind": "uint64", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "rtt", "path": ["rtt"], "index": [5], "type": {"kind": "duration", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1b1de9d8225b12af", "field": "query", "path": ["query"], "index": [6], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
```
