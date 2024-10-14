# write_bitz

Writes `BITZ` (Tenzir's internal wire format)

```tql
write_bitz
```

## Description

Use BITZ when you need high-throughput structured data exchange with minimal
overhead. BITZ is a thin wrapper around Arrow's record batches. That is, BITZ
lays out data in a (compressed) columnar fashion that makes it conducive for
analytical workloads. Since it's padded and byte-aligned, it is portable and
doesn't induce any deserialization cost, making it suitable for
write-once-read-many use cases.

Internally, BITZ uses Arrow's IPC format for serialization and deserialization,
but prefixes each message with a 64 bit size prefix to support changing schemas
between batches—something that Arrow's IPC format does not support on its own.

:::info Did you know?
BITZ is short for **Bi**nary **T**en**z**ir, and a play on the word bits.
:::
