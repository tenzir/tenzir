---
sidebar_position: 5
---

# Show available schemas

When you write a pipeline, you often reference field names. If you do not know
the shape of your data, you can look up available schemas, i.e., the record
types describing top-level events.

Many SQL databases have a `SHOW TABLES` command to show all available table
names, and `SHOW COLUMNS` to display the individual fiels of a given table.

Similarly, our [`show`](../operators/show.md) operator offers the
ability for introspection. Use `show fields` to display all schema fields, with
with one field per event:

```
show fields | where schema == "zeek.dns" | write json -c
```

<details>
<summary>Output</summary>

```json
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "ts", "path": ["ts"], "index": [0], "type": {"kind": "timestamp", "category": "atomic", "lists": 0, "name": "timestamp", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "uid", "path": ["uid"], "index": [1], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": [{"key": "index", "value": "hash"}]}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "orig_h", "path": ["id", "orig_h"], "index": [2, 0], "type": {"kind": "ip", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "orig_p", "path": ["id", "orig_p"], "index": [2, 1], "type": {"kind": "port", "category": "atomic", "lists": 0, "name": "port", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "resp_h", "path": ["id", "resp_h"], "index": [2, 2], "type": {"kind": "ip", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "resp_p", "path": ["id", "resp_p"], "index": [2, 3], "type": {"kind": "port", "category": "atomic", "lists": 0, "name": "port", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "proto", "path": ["proto"], "index": [3], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "trans_id", "path": ["trans_id"], "index": [4], "type": {"kind": "uint", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "rtt", "path": ["rtt"], "index": [5], "type": {"kind": "duration", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "query", "path": ["query"], "index": [6], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "qclass", "path": ["qclass"], "index": [7], "type": {"kind": "uint", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "qclass_name", "path": ["qclass_name"], "index": [8], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "qtype", "path": ["qtype"], "index": [9], "type": {"kind": "uint", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "qtype_name", "path": ["qtype_name"], "index": [10], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "rcode", "path": ["rcode"], "index": [11], "type": {"kind": "uint", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "rcode_name", "path": ["rcode_name"], "index": [12], "type": {"kind": "string", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "AA", "path": ["AA"], "index": [13], "type": {"kind": "bool", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "TC", "path": ["TC"], "index": [14], "type": {"kind": "bool", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "RD", "path": ["RD"], "index": [15], "type": {"kind": "bool", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "RA", "path": ["RA"], "index": [16], "type": {"kind": "bool", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "Z", "path": ["Z"], "index": [17], "type": {"kind": "uint", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "answers", "path": ["answers"], "index": [18], "type": {"kind": "string", "category": "atomic", "lists": 1, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "TTLs", "path": ["TTLs"], "index": [19], "type": {"kind": "duration", "category": "atomic", "lists": 1, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "rejected", "path": ["rejected"], "index": [20], "type": {"kind": "bool", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
{"schema": "zeek.dns", "schema_id": "1581ec5887691e0b", "field": "_write_ts", "path": ["_write_ts"], "index": [21], "type": {"kind": "time", "category": "atomic", "lists": 0, "name": "", "attributes": []}}
```

</details>
