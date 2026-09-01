---
title: Read and parse raw Avro data
type: feature
authors:
  - raxyte
created: 2026-08-31T14:06:19.000000Z
---

The new `read_avro` operator decodes streams of concatenated raw Apache Avro
binary datums.

The new `parse_avro` function decodes one raw Avro datum from each `blob` or
`string` value. Both APIs require the writer schema through the `schema`
argument. They decode raw binary data rather than Avro Object Container Files.
