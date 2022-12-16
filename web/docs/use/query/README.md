# Query

Querying data from VAST (aka *exporting*) involves spinning up a VAST client
that executes a query expression. In the following, we assume that you [set up a
server](/docs/use/run) listening at `localhost:42000`.

To run a query, you need to know (1) what to query, (2) how to express what yout
want, and (3) how to render the result.

## Decide what to query

To figure out what you can query, VAST offers
[introspection](/docs/use/introspect) via the `show` command. You
can start with display all available schemas with fields and type definitions:

```bash
vast show --yaml schemas
```

```yaml
```

## Write a query expression

TODO

## Choose an export format

The *format* defines the encoding of data. ASCII formats include JSON, CSV, or
tool-specific data encodings like Zeek TSV. Examples for binary formats are
PCAP and NetFlow.
