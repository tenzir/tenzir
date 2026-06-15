---
title: Chunk stream operators
type: feature
authors:
  - aljazerzen
  - codex
created: 2026-06-01T09:09:37.255561Z
---

TQL now includes `read_chunks` and `write_chunks` for converting between byte streams and records with a `bytes` field.

For example, you can capture arbitrary output as records and turn it back into a byte stream later:

```tql
from {line: "hello"}, {line: "world"}
write_lines
read_chunks
write_chunks
read_lines
```

This makes it easier to inspect, buffer, transform, and roundtrip chunked byte streams inside a pipeline.
