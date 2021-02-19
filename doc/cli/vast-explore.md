The `explore` command correlates spatially and temporally related activity.

:::note Work In Progress
This documentation does not represent the current state of the `vast explore`
command. Only some of the options shown below are currently implemented.
:::

First, VAST evaluates the provided query expression. The results serve as input
to generate further queries. Specifying temporal constraints (`--after`,
`--before`, or `--context`) apply relative to the timestamp field of the
results.  Specifying spatial constraints can include a join field (`--by`) or a
join expression (`--where`) that references fields from the result set.
Restricting the exploration to specific sets of types (`--for`) works in both
cases.

The `--before`, `--after`, and `--context` parameters create a time box around
every result of the query. For example, this invocation shows all events that
happened up to five minutes after each connection to 192.168.1.10:

```bash
vast explore --after=5min 'zeek.conn.id.resp_h == 192.168.1.10'
```

The `--for` option restricts the result set to specific types. Note that `--for`
cannot appear alone but must occur with at least one other of the selection
options. For example, this invocation shows all DNS requests captured by Zeek up
to 60 seconds after a connection to 192.168.1.10:

```bash
vast explore --after=60s --for=zeek.dns 'zeek.conn.id.resp_h == 192.168.1.10'
```

The `--by` option takes a field name as argument and restricts the set of
returned records to those records that have a field with the same name and where
that field has the same value as the same field in the original record.  In
other words, it performs an equi-join over the given field.

For example, to select all outgoing connections from some address up to five
minutes after a connection to host 192.168.1.10 was made from that address:

```bash
vast explore --after=5min --by=orig_h 'zeek.conn.id.resp_h == 192.168.1.10'
```

The `--where` option specifies a dynamic filter expression that restricts the
set of returned records to those for which the expression returns true.
Syntactically, the expression must be a boolean expression in the VAST query
language. Inside the expression, the special character `$` refers to an element
of the result set. Semantically, the `where` expression generates a new query
for each result of the original query. In every copy of the query, the `$`
character refers to one specific result of the original query.

For example, the following query first looks for all DNS queries to the host
`evil.com` captured by Zeek, and then generates a result for every outgoing
connection where the destination IP was one of the IPs inside the `answer` field
of the DNS result.

```bash
vast explore --where='resp_h in $.answer' 'zeek.dns.query == "evil.com"'
```

Combined specification of the `--where`, `--for`, or `--by` options results in
the intersection of the result sets of the individual options. Omitting all of
the `--after`, `--before` and `--context` options implicitly sets an infinite
range, i.e., it removes the temporal constraint.

Unlike the `export` command, the output format can be selected using
`--format=<format>`. The default export format is `json`.
