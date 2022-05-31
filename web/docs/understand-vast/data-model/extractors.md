# Extractors

*Extractors* describe fields within a schema. The extractor resolution converts
an extractor into an ordered list of *offsets*, which each uniquely describe a
single (possibly nested) field in a given schema. The extractor resolution takes
place once per schema. This enables a consistent way to specify which fields ot
operate on throughout VAST across multiple schemas, e.g., in the [query
language](/docs/understand-vast/query-language/) or the [index
configuration](/docs/setup-vast/tune#tune-sketch-parameters).

Let's look at how extractors work based on a subset of the `zeek.conn` schema:

```yaml
# builtins.yaml
types:
  port:
    type: count
```

```yaml
# zeek.yaml
name: zeek
types:
  conn_id:
    record:
      - orig_h: addr
      - orig_p: port
      - resp_h: addr
      - resp_p: port
  conn:
    record:
      - id: conn_id
      - orig_pkts: count
      # ... and a whole bunch of other fields
```

## Field Extractors

Field extractors have the form `baz` or `foo.bar.baz`, where `foo`, `bar`, and
`baz` describe field names. Field extractors match leaf fields by their
(potentially nested) field name, and may optionally be prefixed with a type
name to reduce redundancy.

As an example, the `zeek.conn.id.orig_h` extractor uniquely describes the
`orig_h` field nested in the `zeek.conn` schema. The `orig_h` extractor
describes all fields of the name `orig_h` regardless of their surrounding
context.

## Type Extractors

A special form of the extractor is the type extractor, which has the form
`:foo.qux`. Type extractors match leaf fields by their type name.

This is best explained on some examples:
- The `:addr` extractor describes all fields of the type `addr`, i.e., the
  fields `orig_h` and `resp_h` in the (possibly nested) `conn_id` record.
- The `:port` extractor describes all fields of type `port`, i.e., the fields
  `orig_p` and `resp_p` in the (possibly nested) `conn_id` record.
- The `:count` extractors describes all fields of the type `count` including
  fields aliases to a count type, i.e., all fields returned by the `:port`
  extractor and the `orig_pkts` field in the `conn` record.

## Wildcards

In an extractor `foo.bar.baz` or `:foo.qux` we call `foo`, `bar`, `baz`, and
`qux` the _sections_ of the extractors.

Every section may be replaced with the wildcard `*`. For example, the extractor
`zeek.*.ts` not only matches the field `zeek.conn.ts`, but also `zeek.dns.ts`
and `zeek.http.ts` (and many others).

## Concepts

import MissingDocumentation from '@site/presets/MissingDocumentation.md';

<MissingDocumentation/>
