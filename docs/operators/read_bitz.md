---
title: read_bitz
category: Parsing
example: 'read_bitz'
---

Parses bytes as *BITZ* format.

```tql
read_bitz
```

## Description

BITZ is short for **Bi**nary **T**en**z**ir and is our internal wire format.

Use BITZ when you need high-throughput structured data exchange with minimal
overhead. BITZ is a thin wrapper around Arrow's record batches. That is, BITZ
lays out data in a (compressed) columnar fashion that makes it conducive for
analytical workloads. Since it's padded and byte-aligned, it is portable and
doesn't induce any deserialization cost, making it suitable for
write-once-read-many use cases.

Internally, BITZ uses Arrow's IPC format for serialization and deserialization,
but prefixes each message with a 64 bit size prefix to support changing schemas
between batches—something that Arrow's IPC format does not support on its own.

## See Also

[`read_bitz`](/reference/operators/read_bitz),
[`read_feather`](/reference/operators/write_feather),
[`read_parquet`](/reference/operators/write_parquet),
[`write_bitz`](/reference/operators/write_bitz)
