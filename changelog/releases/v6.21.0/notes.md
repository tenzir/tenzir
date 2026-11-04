This release hands more of each pipeline to DuckDB, ClickHouse, MySQL, Microsoft SQL Server, and Splunk, so they return only the data that the pipeline keeps. It also fixes several performance problems in parsing and filtering, and makes list membership, OCSF derivation, and context lookups behave consistently.

## 🚀 Features

### Filters, sorting, and limits after summaries in SQL databases

`from_duckdb`, `from_clickhouse`, `from_mysql`, and `from_microsoft_sql` now also run the `where`, `sort`, and `head` that follow a summary they compute, so the database returns only the groups that the pipeline keeps:

```tql
from_clickhouse table="events", host="clickhouse.example.com", tls=false
where severity >= 3
summarize src_ip, events=count()
where events >= 10
sort -events, src_ip
head 20
```

The filter applies to the groups after grouping, and the limit counts groups. This also makes `top` and `rare` followed by `head` return just the top or bottom values from the database. The database runs these steps with its own semantics: text compares and sorts under the collation of its column, and the database decides where `null` sorts, for example first in ascending order in MySQL and SQL Server, and last in DuckDB and ClickHouse. Filters with functions or with constants other than numbers, strings, booleans, and `null` still run in Tenzir, and so does everything after them.

*By @mavam.*

## 🔧 Changes

### Faster Splunk searches with typed results

The `from_splunk` operator now hands as much of the pipeline as it can to Splunk, and returns typed values:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where sourcetype == "linux_secure" and user != "root" and bytes > 1024
summarize src_ip, attempts=count()
```

- **Typed values:** the operator now infers numbers and booleans from the strings that Splunk returns, so `bytes > 1024` and `where success` work as expected. Previously these values stayed strings. Pass `raw=true` to keep every value as a string.
- **All fields:** results carry every extracted field, not only the fields that the search names. The operator appends `| fields *` unless a downstream `select` names the fields that the pipeline needs.
- **Reading indexes:** the new `index` argument reads one or more indexes without writing SPL, so that you can filter in TQL. The `search` argument also accepts searches as typed in Splunk Web, such as `index=main`.
- **Filters, projections, and limits:** downstream `where`, `select`, and `head` go into the search, including negations and IP address filters. Splunk compares some values differently, so these filters only preselect the events, and Tenzir still applies the complete filter.
- **Summaries:** a `summarize` after the operator, also through `top` and `rare`, runs in Splunk with `stats` for group keys, `count()`, `count(field)`, and `min` and `max` of integers. Filters before it go along if Splunk can evaluate them exactly as Tenzir does, such as equality with strings, negations, `null` checks, and comparisons of integers, as in the example above. Whenever Splunk's result could differ from what `summarize` computes, Tenzir summarizes the events itself instead.

Searches with `fieldformat`, macros, or commands with side effects such as `outputlookup` keep the respective work in Tenzir. The reference page on SPL optimizations lists the exact rules.

*By @mavam.*

### Filters through ocsf_cast and field selection functions

Filters on fields of the result of `ocsf_cast`, `select_matching`, and `drop_matching` now move in front of the call and read its input. Behind `this = ocsf_cast(this)`, a filter such as `where src_endpoint.hostname == "dc01"` passes the cast and the mapping before it, so that it reaches the source. Once the mapping assigns `class_uid` and `metadata.version` as constants, a comparison of a copied field with a constant becomes a plain comparison of the mapped input field, which sources such as `read_parquet` or databases evaluate, plus a local check that the value has the type that the cast requires.

*By @mavam.*

### Native DuckDB semantics for operators after from_duckdb

`from_duckdb` now hands DuckDB the operators that follow it in a `table` read, as long as DuckDB can spell them: `where`, `select`, `drop`, `summarize`, `sort`, `head`, and `top` or `rare` followed by `head`. Those operators leave the pipeline and run in DuckDB with DuckDB's semantics. For example, `name != "alpha"` no longer keeps rows where `name` is `null`, text compares under the collation of its column, and `null` sorts last:

```tql
from_duckdb "events.duckdb", table="events"
where severity >= 3 and src_ip == 10.0.0.1
select src_ip, host=meta.host
sort -src_ip
head 100
```

Before, `from_duckdb` pushed only the filters and summaries that DuckDB computes exactly as TQL does, and computed the rest itself. A `live` read hands DuckDB its filters only.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics. Column pruning still applies:

```tql
from_duckdb "events.duckdb", table="events", pushdown=false
where name != "alpha"
```

A query that DuckDB rejects now fails the pipeline, for example a `select` of a column that the table lacks.

*By @mavam.*

### Pushdown through more stateful operators

Pipeline optimization now carries field selection through `unroll`, `sample`, `reorder`, `delay`, `throttle`, `assert`, `assert_throughput`, `ai_prompt`, and `ai_decide`, so sources and readers such as `read_parquet` materialize only the fields that downstream operators and these operators use.

`unroll` also lets filters on other fields run before it. `delay` and `throttle` without `drop` pass a downstream `head` upstream, so readers can stop early. `ai_prompt` and `ai_decide` let filters on fields other than their result run before them, so they send no requests for the events that these filters drop. Without a filter on the result, they also pass a downstream `head` upstream, so sources that support limits, such as `read_parquet`, stop early.

*By @mavam.*

## 🐞 Bug fixes

### Batch-independent list handling in ocsf_derive

`ocsf_derive` treats an enum pair as inconsistent when one of its fields is a list and the other is not, and keeps both fields unchanged. It used to apply this rule only when other events in the same batch had lists in both fields, so the result for an event could depend on the events around it. It now applies the rule to every event, so that `{activity_id: [1], activity_name: null}` keeps its `null` name regardless of the other events.

*By @mavam.*

### Consistent key resolution in context_lookup

With the new data model, `context_lookup` now resolves its `key` fields the same way as with the old data model.

*By @IyeOnline.*

### Consistent list membership for booleans and numbers

The `in` operator no longer matches booleans with numbers. It now holds exactly where `==` holds for some element of the list, so `false` no longer matches `0`, and `true` no longer matches `1`:

```tql
from {x: false}, {x: 0}, {x: 5}
where x in [0, 5]
```

```tql
{x: 0}
{x: 5}
```

Previously, the filter also kept `{x: false}`, although `false == 0` is `false`. Numbers still match across their types, so `5 in [5.0]` remains `true`. As with `==`, a comparison with a secret is now `null` with a warning rather than `false`.

*By @mavam.*

### Faster parsing with schemas

Parsing with a schema no longer slows down as batches grow. This affects functions and operators that apply a schema to the events they build, such as `parse_syslog` and `read_syslog`, which do so by default. Previously, building each event took time proportional to the number of events before it in the batch, so large batches parsed many times slower than small ones.

*By @jachris.*

### Filters no longer repeat expensive work

Filters that read a computed field, such as `where s.app_name == "sshd"` after `s = line.parse_syslog()`, no longer slow down the pipeline. Previously, the optimizer moved such filters in front of the assignment by copying the computation into the filter, so the pipeline parsed every event once more, and once per field that the filter read. Chained assignments could multiply the copies, which made some pipelines many times slower. The same applied to filters inside `if` and `match` branches whose condition was expensive. Such filters now stay right behind the expensive operation, while filters on plain fields and cheap conversions still move towards the source.

*By @jachris.*

### Ordered input for taste and throttle with drop

`taste` and `throttle drop=true` keep the first events they see, but the optimizer could reorder their input, for example by removing a preceding `sort`. They now keep the first events in the original order, so `sort x | taste 1` returns the event with the smallest `x`.

*By @mavam.*

### Package operator diagnostics during node startup

Node startup now reports package operator and constant definition errors with their file, line, and source excerpt. This makes it easier to identify the cause of later `module not found` errors. Startup continues when the unavailable operators are unused; pipelines that reference them still fail validation.

*By @zedoraps.*

### Reliable delivery of split string results

Event batches containing results from `split()` or `split_regex()` now reach subscribers reliably. Previously, some batches with repeated strings or removed separators could produce no output after `publish`, or fail a `write_bitz`/`read_bitz` round trip.

*By @zedoraps.*

### Stable partition layouts for sparse events

Importing events whose schema has many optional fields, such as Suricata EVE events, no longer creates a new partition layout for every combination of fields that happen to be null. Fields that are null throughout a partition now take their type from the registered schema of the events, so partitions of the same schema share one layout again.

*By @IyeOnline.*
