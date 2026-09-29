---
title: Avro binary serialization
type: feature
authors:
  - aljaz
created: 2026-09-21T14:35:21.309795Z
---

Use the new `print_avro` function to encode TQL values as Avro binary blobs with an explicit writer schema for message sinks such as Kafka:

```tql
from {id: 42, name: "alice"}
message = print_avro(this, schema={
  type: "record",
  name: "event",
  fields: [
    {name: "id", type: "long"},
    {name: "name", type: "string"},
  ],
})
to_kafka "events", message=message
```

The required `schema` argument accepts a constant TQL record or an Avro JSON schema string. Record fields are encoded in schema order, and missing or extra fields are rejected. Unions select the first matching branch; include a `"null"` branch to encode null values. Values that do not match the schema produce null and a warning.

The output contains no container header, schema, or schema registry framing, so consumers need the matching writer schema. Logical type annotations do not convert units: times and durations encode as nanoseconds unless you convert the input first.
