# Export

Querying data from VAST (aka *exporting*) involves spinning up a VAST client
that executes a query expression. In the following, we assume that you [set up a
server](../run/README.md) listening at `localhost:5158`.

To run a query, you need to:

1. know what to look for
2. express what you want
3. decide in what format to show the result

Let's go through each of these steps.

## Decide what to query

To figure out what you can query, VAST offers
[introspection](../introspect/README.md) via the `show` command.

Use `show schemas` to display the schema of all types:

```bash
vast show schemas --yaml
```

In case you ingested [Suricata](../../understand/formats/suricata.md) data, this
may print:

```yaml
- suricata.flow:
    record:
      - timestamp:
          timestamp: time
      - flow_id:
          type: uint64
          attributes:
            index: hash
      - pcap_cnt: uint64
      - vlan:
          list: uint64
      - in_iface: string
      - src_ip: ip
      - src_port:
          port: uint64
      - dest_ip: ip
      - dest_port:
          port: uint64
      - proto: string
      - event_type: string
      - community_id:
          type: string
          attributes:
            index: hash
      - flow:
          suricata.component.flow:
            record:
              - pkts_toserver: uint64
              - pkts_toclient: uint64
              - bytes_toserver: uint64
              - bytes_toclient: uint64
              - start: time
              - end: time
              - age: uint64
              - state: string
              - reason: string
              - alerted: bool
      - app_proto: string
- suricata.http:
    record:
      - timestamp:
          timestamp: time
      - flow_id:
          type: uint64
          attributes:
            index: hash
      - pcap_cnt: uint64
      - vlan:
          list: uint64
      - in_iface: string
      - src_ip: ip
      - src_port:
          port: uint64
      - dest_ip: ip
      - dest_port:
          port: uint64
      - proto: string
      - event_type: string
      - community_id:
          type: string
          attributes:
            index: hash
      - http:
          record:
            - hostname: string
            - url: string
            - http_port:
                port: uint64
            - http_user_agent: string
            - http_content_type: string
            - http_method: string
            - http_refer: string
            - protocol: string
            - status: uint64
            - redirect: string
            - length: uint64
      - tx_id:
          type: uint64
          attributes:
            index: hash
```

The next section discusses how you can refer to various elements of this type
schema.

## Begin with an expression

We designed the [VAST language](../../understand/README.md) to make it
easy to reference the data schema and put constraints on it. Specifically,
VAST's [expression language](../../understand/expressions.md) has the
concept of [extractors](../../understand/expressions.md#extractors)
that refer to various parts of the event structure. For example, you can query
the above schemas with a *meta extractor* to select a specific set of event
types:

```c
#type == /suricata.(http|flow)/
```

This predicate restricts a query to the event types `suricata.flow` and
`suricata.http`. You can think of the meta extractor as operating on the table
header, whereas *field extractors* operate on the table body instead:

```c
hostname == "evil.com" || dest_ip in 10.0.0.0/8
```

This expression has two predicates with field extractors. The first field
extractor `hostname` is in fact a suffix of the fully-qualified field
`suricata.http.hostname`. Because it's often inconvenient to write down the
complete field name, you can write just `hostname` instead. Of there exist
multiple fields that qualify, VAST builds the logical OR (a *disjunction*) of
all fields. This may unfold as follows:

```c
suricata.http.hostname == "evil.com" || myevent.hostname == "evil.com" || ...
```

So at the end it's up to you: if you want to be fast and can live with
potentially cross-firing other matches, then you can go with the "short and
sweet" style of writing your query. If you need exact answers, you can always
write out the entire field.

Looking at the other side of the field name, we have its type. This is where
*type extractors* come into play. In you don't know the field name you are
looking for, we still want that you can write queries effectively. Taking the
above query as an example, you can also write:

```c
:string == "evil.com" || :ip in 10.0.0.0/8
```

In fact, both predicates in this expression are what we call [value
predicates](../../understand/expressions.md#value-predicates), making
it possible to shorten this expression to:

```c
"evil.com" || 10.0.0.0/8
```

Using type extractors (and thereby value predicates) hinges on having
a powerful type system. If you only have strings and numbers, this is not
helping much. VAST's [type system](../../understand/data-model/type-system.md)
supports *aliases*, e.g., you can define an alias called `port` that points to a
`uint64`. Then you'd write a query only over ports:

```c
:port != 443
```

As above, this predicate would apply to all fields of type `port`—independent of
their name.

To summarize, we have now seen three ways to query data, all based on
information that is intrinsic to the data. There's another way to write queries
using extrinsic information: [event taxonomies][taxonomies], which define
*concepts* and *models*. Concepts are basically field mappings that VAST
resolves prior to query execution, whereas models define a tuple over concepts,
e.g., to represent common structures like a network connection 4-tuple. A
concept query looks syntactically identical to field extractor query. For
example:

```c
net.src.ip !in 192.168.0.0/16
```

VAST resolves the concept `net.src.ip` to all fieldnames that this concept has
been defined with. We defer to the [taxonomy documentation][taxonomies] for a
more detailed discussion.

[taxonomies]: ../../understand/data-model/taxonomies.md

## Apply a pipeline

After providing a filter expression, you can optionally continue with a
[pipeline](../../understand/pipelines.md).

```cpp
src_ip == 192.168.1.104
| select timestamp, flow_id, src_ip, dest_ip, src_port
| drop timestamp
```

## Choose an export format

After your have written your query expression, the final step is selecting how
you'd like the result to be served. The `export` command spins up a VAST client
that connects to a server where the query runs, and receives the results back to
then render them on standard output:

```bash
vast export [options] <format> [options] [expr]
```

The [format](../../understand/formats/README.md) defines how VAST renders the
query results. Text formats include [JSON](../../understand/formats/json.md),
[CSV](../../understand/formats/csv.md), or tool-specific data encodings like
[Zeek](../../understand/formats/zeek.md).
[PCAP](../../understand/formats/pcap.md) is an example for a binary format.

For example, to run query that exports the results as JSON, run:

```bash
vast export json net.src.ip in 10.0.0.0/8
```
