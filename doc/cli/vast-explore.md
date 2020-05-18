The `vast explore` command correlates spatially and temporally related
activity.

:::note Work In Progress
This documentation does not represent the current state of the `vast explore`
command. Only some of the options shown below are currently implemented.
:::

First, VAST evaluates the provided query expression. The results serve as input
to generate further queries. Specifying temporal constraints (`--after`,
`--before`, or `--context`) apply relative to the #timestamp field of the
results.  Specifying spatial constraints can include a join field (`--by`) or a
join expression (`--where`) that references fields from the result set.
Restricting the exploration to specific sets of types (`--for`) works in both
cases.

The `--before`, `--after`, and `--context` parameters create a time box around
every result of the query. For example, this invocation shows all events that
happened up to five minutes after each connection to 192.168.1.10:

```bash
vast explore -A 5min 'zeek.conn.id.resp_h == 192.168.1.10'
```

The `--for` option restricts the result set to specific types. Note that
`--for` cannot appear alone but must occur with at least one other of the
selection options. For example, this invocation shows all DNS requests captured
by Zeek up to 60 seconds after a connection to 192.168.1.10:

```bash
vast explore -A 60s --for=zeek.dns 'zeek.conn.id.resp_h == 192.168.1.10'
```

The `--by` option takes a field name as argument and restricts the set of
returned records to those records that have a field with the same name and
where that field has the same value as the same field in the original record.
In other words, it performs an equi-join over the given field.

For example, to select all outgoing connections from some address up to five
minutes after a connection to host 192.168.1.10 was made from that address:

```bash
vast explore -A 5min --by=orig_h 'zeek.conn.id.resp_h == 192.168.1.10'
```

The `--where` option specifies a dynamic filter expression that restricts the
set of returned records to those for which the expression returns true.
Syntactically, the expression must be a boolean expression in the VAST query
language. Inside the expression, the special character `$` refers to an element
of the result set. Semantically, the `where` expression generates a new query
for each result of the original query. In every copy of the query, the ‘$’
character refers to one specific result of the original query.

For example, the following query first looks for all DNS queries to the host
`evil.com` captured by Zeek, and then generates a result for every outgoing
connection where the destination IP was one of the IPs inside the `answer`
field of the DNS result.

```bash
vast explore --where='resp_h in $.answer' 'zeek.dns.query == "evil.com"'
```

Combined specification of the `--where`, `--for`, or `--by` options results in
the intersection of the result sets of the individual options. Omitting all of
the `--after`, `--before` and `--context` options implicitly sets an infinite
range, i.e., it removes the temporal constraint.

OPTIONS
-------

**Temporal Selection**

`-A, --after=DURATION`:

Restricts the result set to those records with a timestamp in the interval [t,
t+DURATION), where t is the timestamp of a result of the original query.

`-B, --before=DURATION`:

Restricts the result set to those records with a timestamp in the interval
(t-DURATION, t], where t is the timestamp of a result of the original query.

`-C, --context=DURATION`:

Restricts the result set to those records with a timestamp in the interval
(t-DURATION, t+DURATION), where t is the timestamp of a result of the original
query.

**Spatial Selection**

`--where=EXPRESSION`:

Restricts the result set to those records for which the EXPRESSION evaluates to
true. The EXPRESSION is a boolean expression. It uses the same syntax as the
vast query language, with the addition of the special character `$` that can be
used to refer to the result of the QUERY.

`--for=RECORD_TYPES`:

Restricts the result set to those record types listed in the list RECORD_TYPES.
This option must always be accompanied by at least one other of the selection
options.

`--by=FIELD`:

Restricts the result set to those records that have a field named FIELD and
where the value of that field equals the value of the field with the same name
in the query result.

**Output**

`--format=FORMAT`:

Selects the output format of the explore command. Valid values are `json`,
`arrow`, `csv` and `ascii`.
