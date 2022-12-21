# Query

Querying data from VAST (aka *exporting*) involves spinning up a VAST client
that executes a query expression. In the following, we assume that you [set up a
server](/docs/use/run) listening at `localhost:42000`.

To run a query, you need to:

1. know what to look for
2. express what you want
3. decide in what format to show the result

Let's go through each of these steps.

## Decide what to query

To figure out what you can query, VAST offers
[introspection](/docs/use/introspect) via the `show` command.

Use `show schemas` to display the layout of all types:

```bash
vast show schemas --yaml
```

In case you ingested [Suricata](/docs/understand/formats/suricata) data, this
may print:

```yaml
- suricata.flow:
    record:
      - timestamp:
          timestamp: time
      - flow_id:
          type: count
          attributes:
            index: hash
      - pcap_cnt: count
      - vlan:
          list: count
      - in_iface: string
      - src_ip: addr
      - src_port:
          port: count
      - dest_ip: addr
      - dest_port:
          port: count
      - proto: string
      - event_type: string
      - community_id:
          type: string
          attributes:
            index: hash
      - flow:
          suricata.component.flow:
            record:
              - pkts_toserver: count
              - pkts_toclient: count
              - bytes_toserver: count
              - bytes_toclient: count
              - start: time
              - end: time
              - age: count
              - state: string
              - reason: string
              - alerted: bool
      - app_proto: string
- suricata.http:
    record:
      - timestamp:
          timestamp: time
      - flow_id:
          type: count
          attributes:
            index: hash
      - pcap_cnt: count
      - vlan:
          list: count
      - in_iface: string
      - src_ip: addr
      - src_port:
          port: count
      - dest_ip: addr
      - dest_port:
          port: count
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
                port: count
            - http_user_agent: string
            - http_content_type: string
            - http_method: string
            - http_refer: string
            - protocol: string
            - status: count
            - redirect: string
            - length: count
      - tx_id:
          type: count
          attributes:
            index: hash
```

The next section discusses how you can refer to various elements of this type
layout.

## Write a query expression

We designed the [**VAST** **Q**uery **L**anguage
(VASTQL)](/docs/understand/query-language) to make it easy to reference the data
layout and put constraints on it. Specifically, VAST's expression language has
the concept of
[extractors](/docs/understand/query-language/expressions#extractors) that refer
to various parts of the event structure:

![Extractors #width500](/img/extractors.light.png#gh-light-mode-only)
![Extractors #width500](/img/extractors.dark.png#gh-dark-mode-only)

For example, you can query the above schemas with a *meta extractor* to select a
specific set of event types:

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
:string == "evil.com" || :addr in 10.0.0.0/8
```

In fact, both predicates in this expression are what we call [value
predicates](/docs/understand/query-language/expressions#value-predicates),
making it possible to shorten this expression to:

```c
"evil.com" || 10.0.0.0/8
```

Using type extractors (and thereby value predicates) hinges on having
a powerful type system. If you only have strings and numbers, this is not
helping much. VAST's [type system](/docs/understand/data-model/type-system)
supports *aliases*, e.g., you can define an alias called `port` that points to a
`count`. Then you'd write a query only over ports:

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

[taxonomies]: /docs/understand/data-model/taxonomies

## Choose an export format

After your have written your query expression, the final step is selecting how
you'd like the result to be served. The `export` command spins up a VAST client
that connects to a server where the query runs, and receives the results back to
then render them on standard output:

```bash
vast export [options] <format> [options] [expr]
```

The [format](/docs/understand/formats) defines how VAST renders the query
results. Text formats include [JSON](/docs/understand/formats/json),
[CSV](/docs/understand/formats/csv), or tool-specific data encodings like
[Zeek](/docs/understand/formats/zeek). [PCAP](/docs/understand/formats/pcap) is
an example for a binary format.

For example, to run query that exports the results as JSON, run:

```bash
vast export json net.src.ip in 10.0.0.0/8
```
