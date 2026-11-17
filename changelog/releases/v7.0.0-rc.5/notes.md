This release candidate rebuilds Tenzir with a new platform and interface, a new deployment model with configuration management, versioned packages, and live rollout, an MCP server for AI agents, and a built-in assistant. Tenzir Node v6 continues to receive maintenance releases until the migration path is ready.

## 💥 Breaking changes

### Platform and deployments reimagined

Meet the next generation of Tenzir: a rebuilt Tenzir Platform, a new deployment model, an MCP server for your AI agents, and a built-in assistant that helps you along the way.

Want to try it out, ask questions, or share feedback? Join us on Discord at https://discord.tenzir.com/.

**New platform and interface.** The Tenzir Platform comes with a redesigned web interface for building, running, and observing your pipelines. On the command line, `tenzir` runs pipelines locally with `tenzir run` and manages your deployments, configurations, and packages from one place.

**Better configuration management.** Each deployment runs one configuration that holds its pipelines, packages, and secrets. Edit it in the web interface or with the `tenzir` command, and always know what a deployment runs.

**Versioned packages with dependencies.** Packages now have versions and can depend on each other, with a lock file pinning exactly what your deployments run.

**Live configuration rollout.** The new deployment model applies configuration changes to running deployments live. Change a pipeline, and your deployments pick it up.

**MCP.** The platform speaks the Model Context Protocol, so AI agents can inspect and manage your deployments and pipelines directly.

**Assistant.** A built-in assistant in the web interface helps you write TQL, build pipelines, and operate your deployments.

A migration path from Tenzir Node to the new deployment model is planned, but not available yet. Until then, we continue to publish maintenance releases for Tenzir Node v6.

*By @aljazerzen, @Avaq, @gitryder, @IyeOnline, @jachris, @lava, @mavam, @raxyte, @tobim, and @zedoraps.*

## 🚀 Features

### Get help with tenzir help

Run `tenzir help` to list all commands, and `tenzir help <command>` for the flags and examples of one, such as `tenzir help config apply`. This works the same as `tenzir --help` and `tenzir <command> --help`, except for `run`: `tenzir help run` describes local and remote runs, and `tenzir run --help` lists the options of a local run.

The examples in the help now show what you can do with each command, starting with a typical session in `tenzir help`.

*By @mavam.*

### Homebrew puts tenzir on PATH

Installing Tenzir with `brew install --cask tenzir/tenzir/tenzir` now links the `tenzir` command into Homebrew's `bin` directory, so it is on your `PATH` without adding `/opt/tenzir/bin` by hand. Uninstalling the cask removes the link.

*By @mavam.*

### Run SPL2 searches with from_splunk

`from_splunk` now runs SPL2 searches with the new `spl2` argument, as an alternative to `search` for SPL and `index`. SPL2 searches need Splunk Enterprise 10.2 or Splunk Cloud Platform 10.2.2510 or later.

As for SPL, `from_splunk` hands Splunk the operators that follow it: `where`, `select`, `drop`, `summarize`, `sort`, and `head` run in Splunk with Splunk's semantics. For example, this pipeline:

```tql
from_splunk "https://splunk.example.com:8089",
  spl2=r#"from main where sourcetype == "linux_secure""#,
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where user != "root"
summarize user, attempts=count()
sort -attempts
head 20
```

dispatches the SPL2 search below as the statement `$main` of a module, and exports its results once the search job is done:

```
from main where sourcetype == "linux_secure"
| where ('user' != "root")
| stats count() as tenzir_column_0 by 'user'
| sort 20 -'tenzir_column_0'
| head 20
| appendpipe [stats count() as tenzir_groups_0]
```

Filters that compare `_time` with a time, and the operators after them, stay in the pipeline, since an SPL2 search may redefine `_time`. The `options` argument does not apply to `spl2`. Use `pushdown=false` to keep every operator in the pipeline with TQL semantics.

*By @mavam.*

## 🔧 Changes

### Content-skipping filters and limits after from_microsoft_365_activity

`from_microsoft_365_activity` now uses the operators after it to skip downloads. The Management Activity API cannot filter events, but every event of a content blob carries the same metadata. A `where` on `microsoft_365_activity` therefore decides which blobs the operator downloads, and `head` stops listing and downloading:

```tql
from_microsoft_365_activity auth={
    tenant_id: secret("m365-tenant-id"),
    client_id: secret("m365-client-id"),
    client_secret: secret("m365-client-secret"),
  },
  content_types=["Audit.Exchange", "Audit.SharePoint"],
  start_time=2026-06-01T00:00:00Z,
  end_time=2026-06-02T00:00:00Z
where microsoft_365_activity.content_type == "Audit.Exchange"
where microsoft_365_activity.content_created >= 2026-06-01T12:00:00Z
head 1000
```

This pipeline lists only `Audit.Exchange` content from 12:00 on, and stops after 1,000 events. The filters keep TQL semantics, so the results stay the same. Use `pushdown=false` to download every blob.

*By @mavam.*

### Exact distinct counts run in KQL sources

`from_microsoft_defender` and `from_azure_log_analytics` now compute `count_distinct` in the service, such as how many accounts signed in to each computer:

```tql
from_azure_log_analytics "SecurityEvent",
  workspace_id=secret("log-analytics-workspace"),
  azure_auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("log-analytics-client-secret"),
  }
summarize Computer, accounts=count_distinct(Account)
```

KQL computes `accounts` exactly with `count_distinctif(Account, isnotempty(Account))`, not with the approximate `dcount`. Like `count`, it leaves out empty strings, which KQL holds in place of `null`. Strings compare case-sensitively. The service fails a query that counts more than 100 million distinct values per group, as well as one that counts `dynamic` values. Use `pushdown=false` to count them in Tenzir instead.

*By @mavam.*

### KQL sources project fields also without pushdown

With `pushdown=false`, `from_microsoft_defender` and `from_azure_log_analytics` now keep only the fields that the operators after them read, as other sources do. For example:

```tql
from_microsoft_defender "DeviceEvents", azure_auth={…}, pushdown=false
where DeviceName == "host-a"
select DeviceName, Id
```

The query now ends in `| project ['Id'] = column_ifexists('Id', dynamic(null)), ['DeviceName'] = column_ifexists('DeviceName', dynamic(null))`, while the `where` and the `select` still run in Tenzir. Before, the query went out unchanged.

*By @mavam.*

### More aggregation functions run in the source

Sources now hand their targets more aggregation functions of a `summarize`: `count_if`, `count_distinct`, `sum`, and `mean`, where the target has an exact equivalent. For example:

```tql
from_microsoft_sql table="events", host="db", user="u", password=secret("p"), database="d"
summarize big=count_if(n, v => v > 0), kinds=count_distinct(n), total=sum(id), avg=mean(id)
```

SQL Server computes this as `COUNT_BIG(CASE WHEN (([n] IS NOT NULL) AND ([n] > 0)) THEN [n] END)`, `COUNT_BIG(DISTINCT [n])`, `SUM([id])`, and `AVG(CAST([id] AS float))`. DuckDB and MySQL compute all but `sum`, ClickHouse and Hydrolix all four, Splunk all four with `count(eval(...))`, `dc`, `sum`, and `avg`, and Microsoft Defender and Azure Log Analytics `count_if` with `countif`. SentinelOne computes `count_distinct` with the approximate `estimate_distinct`, since PowerQuery has no exact distinct count.

*By @mavam.*

### Native CloudWatch Logs semantics for operators after from_amazon_cloudwatch

`from_amazon_cloudwatch` now hands CloudWatch Logs the operators after it that its request can express: filter patterns, time windows, log streams, and limits. They leave the pipeline and run in CloudWatch Logs, with its semantics. For example, this pipeline:

```tql
from_amazon_cloudwatch "/aws/lambda/checkout", mode="search"
where "ERROR" in message
where not ("timeout" in message)
where timestamp >= 2026-01-01T12:00:00Z
where log_stream in ["2026/01/01/[$LATEST]abc", "2026/01/01/[$LATEST]def"]
head 100
```

sends this `FilterLogEvents` request:

```json
{
  "logGroupName": "/aws/lambda/checkout",
  "logStreamNames": ["2026/01/01/[$LATEST]abc", "2026/01/01/[$LATEST]def"],
  "startTime": 1767268800000,
  "filterPattern": "\"ERROR\" -\"timeout\"",
  "limit": 100
}
```

With `mode="live"`, `where "ERROR" in message or "FATAL" in message` becomes the Live Tail pattern `?"ERROR" ?"FATAL"`. A live read also warns when Live Tail samples the matching events. Use `pushdown=false` to keep every operator in the pipeline with TQL semantics.

*By @mavam.*

### Native Hydrolix semantics for operators after from_hydrolix

`from_hydrolix` now hands Hydrolix the operators that follow it in a `table` read, as `from_clickhouse` does for ClickHouse: `where`, `select`, `drop`, `summarize` with `count`, `min`, and `max`, `sort`, `head`, and `top` or `rare` followed by `head`, including the operators after a `summarize`. These operators leave the pipeline and run in Hydrolix with the semantics of its ClickHouse engine. For example, a comparison with `NULL` yields `NULL`, so `not (n == 1)` and `s != "a"` no longer keep rows where the field is `null`. Before, `from_hydrolix` added some filters and summaries to its query and ran everything else in Tenzir.

A query that Hydrolix rejects fails the pipeline with Hydrolix's error, for example when it compares a column with a value of another type. A field that the table lacks in the expected shape, such as an element of a column that is no tuple, fails the read before it runs any query. Both errors suggest `pushdown=false`, which keeps every operator in the pipeline with TQL semantics:

```tql
from_hydrolix "https://acme.hydrolix.live", token=secret("hydrolix-token"), table="logs.events", pushdown=false
where not (severity == 3)
```

Hydrolix reads only the partitions within the time range that a query sets on the table's primary timestamp. Put a filter on that timestamp directly after `from_hydrolix`, so that it constrains the table itself:

```tql
let $since = now() - 1h
from_hydrolix "https://acme.hydrolix.live", token=secret("hydrolix-token"), table="logs.events"
where timestamp > $since
summarize src_ip, events=count()
```

A filter after a `head` or a `summarize` reads the result of that operator and does not restrict the time range. With `pushdown=false`, no filter reaches the query, so a cluster that requires a time range rejects it. In that case, read with `sql` and a query that sets the time range itself.

A field `a.b` now names the element `b` of the tuple `a`, while `this["a.b"]` names the column `a.b`. For a table with a catch-all column, Hydrolix runs `where`, `sort`, and `head` on the fields of the reconstructed events, as long as each field comes from a single column, but a `select`, `drop`, or `summarize` fails the read and suggests `pushdown=false`.

A field below a map column reads the value of its key in Hydrolix. For example, `where catchall.class_uid == "1001"` reads `catchall['class_uid']` from the `catchall` column of the type `Map(String, Nullable(String))`, which keeps the fields that Hydrolix's default transform does not map. A missing key yields the default value of the map's values: `null` for nullable values, so `catchall.missing == null` holds, but an empty string or `0` otherwise, where Tenzir would see no field. Only maps with string keys have fields that operators can address.

Hydrolix may reject a query that reads a field below a `JSON` column in one of the subqueries that `from_hydrolix` generates, for example for a `select` or for the operators after a `head`. Use `pushdown=false` in that case, or write the query yourself with `sql`, including any `SETTINGS` that your cluster needs. A query with `sql` runs as you wrote it, with the settings of your session.

*By @mavam.*

### Native Microsoft Graph semantics for operators after from_microsoft_graph

`from_microsoft_graph` now hands Microsoft Graph the `where`, `select`, and `head` operators after it as OData query parameters. They leave the pipeline and run in Graph, with Graph's semantics. For example, this pipeline:

```tql
from_microsoft_graph "users",
  auth={
    tenant_id: secret("graph-tenant-id"),
    client_id: secret("graph-client-id"),
    client_secret: secret("graph-client-secret"),
  }
where department == "Security"
where accountEnabled == true
select id, name=displayName, mail
head 50
```

requests a single page:

```text
/v1.0/users?$filter=(department eq 'Security') and (accountEnabled eq true)&$select=id,displayName,mail&$top=50
```

Graph may silently ignore filters that it does not support, so `where` becomes `$filter` only for the resources and properties for which Microsoft documents `$filter` support. Use `pushdown=false` to keep every operator in the pipeline with TQL semantics.

*By @mavam.*

### Operators after from_velociraptor stay in the pipeline by design

`from_velociraptor` now states in the optimized pipeline why it takes over none of the operators after it: a VQL query may have effects, such as collecting artifacts or subscribing to events, that a filter or limit in the query would change, and `max_rows` only sizes the response batches. The operators after it keep running in the pipeline as before.

*By @mavam.*

### Pipe sources compute the values that a summary reads

`from_splunk`, `from_microsoft_defender`, and `from_azure_log_analytics` now also take over a `summarize` that groups by or aggregates computed values, such as values that a `set` before it assigns:

```tql
from_splunk "https://splunk:8089", index="main", earliest="-1h", latest="now"
kind = action.to_upper()
summarize kind, events=count()
```

Splunk computes the key with `| eval tenzir_column_0=upper('action')` before its `stats`, and KQL with `| extend` before its `summarize`.

*By @mavam.*

### Sink byte metrics count the delivered payload

Sink operators now report the bytes they write consistently: the payload before any compression they apply, counted once it was delivered, and once however many retries it took. Together with sources, which report the bytes they read, this makes the bytes going into and out of a pipeline comparable.

For example, `to_splunk`, `to_hydrolix`, `to_opensearch`, `to_prometheus`, `to_google_secops`, `to_azure_log_analytics`, and `to_sentinelone_data_lake` now count the request body before compressing it, and only once the destination accepted it. `to_google_secops` counts nothing in `dry_run` mode. `to_google_cloud_logging` counts entries once Cloud Logging accepted them, `to_http` with `buffer_all` counts the body once the request succeeded, and `to_kafka` counts a message once the brokers acknowledged it.

*By @mavam.*

### SQL sources compute the values that a summary reads

SQL sources now take over a `summarize` that reads computed values, including values that a `set` before it assigns. `from_duckdb`, `from_mysql`, `from_microsoft_sql`, `from_clickhouse`, and `from_hydrolix` then compute those values with the semantics of the database:

```tql
from_duckdb "events.duckdb", table="events"
level = severity * 2
summarize src_ip, worst=max(level)
```

DuckDB computes `worst` as `MAX(("severity" * 2))`. An assignment that the summary does not read keeps the operators in the pipeline if it may fail, such as a function call.

*By @mavam.*

### Tighter time ranges from filters

Sources that narrow the time range of their requests to the filters after them now share one reading of those filters. A strict bound now excludes its time, so `from_splunk` requests `latest_time=1735689720` instead of `1735689721` for `where _time < 2025-01-01T00:02:00Z`. `from_microsoft_365_activity` now also narrows the listing to the range that encloses an `or`, such as for `where microsoft_365_activity.content_created >= 2024-01-02 or microsoft_365_activity.content_created == 2024-01-01`, and narrows the side of an `==` that it can represent.

*By @mavam.*

### Time buckets run in KQL sources and SentinelOne

`from_microsoft_defender`, `from_azure_log_analytics`, and `from_sentinelone_data_lake` now compute time buckets with `floor` in the service, such as for the groups of a `summarize`. Only the buckets travel to Tenzir, not the events in them:

```tql
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-token"),
  start=2026-11-16T00:00:00Z,
  end=2026-11-17T00:00:00Z
bucket = floor(timestamp, 7h)
summarize bucket, events=count()
sort bucket
```

```tql
{bucket: 2026-11-15T19:00:00Z, events: 12744}
{bucket: 2026-11-16T02:00:00Z, events: 45996}
{bucket: 2026-11-16T09:00:00Z, events: 44480}
{bucket: 2026-11-16T16:00:00Z, events: 44221}
{bucket: 2026-11-16T23:00:00Z, events: 6378}
```

On a test tenant, this pipeline summarizes a day of some 150,000 events in about 6 seconds. With `pushdown=false`, reading the same events into Tenzir took longer than the default timeout of 3 minutes. Over shorter windows, both produce the same buckets.

SentinelOne computes `bucket` with `timebucket(timestamp, "25200s")`, and KQL with `bin`, as in `bin(TimeGenerated, timespan(36000000000tick))` for `floor(TimeGenerated, 1h)`. The buckets count from the epoch, as in TQL, also before 1970. KQL's `bin` counts from 0001-01-01, which agrees with the epoch only for some widths, such as those that divide two days, so other widths stay in the pipeline for Defender and Log Analytics. Widths that are multiples of a week or a month stay in the pipeline everywhere, since TQL aligns them to the calendar, and so do widths below a second in SentinelOne.

A `timespan` literal of more than 2^53 ticks, some 28 years, now stays in the pipeline, since KQL reads it inexactly.

*By @mavam.*

### Time buckets run in SQL sources

`from_duckdb`, `from_microsoft_sql`, `from_clickhouse`, and `from_hydrolix` now compute time buckets with `floor` in the database, such as for the groups of a `summarize`:

```tql
from_clickhouse table="events", host="localhost"
hour = floor(time, 1h)
summarize hour, events=count()
```

The buckets count from the epoch, as in TQL, also before 1970. ClickHouse computes `hour` with flooring division of `toUnixTimestamp64Nano`, and SQL Server with `DATE_BUCKET(second, 3600, …, CAST('1970-01-01' AS datetime2(7)))`, which needs SQL Server 2022. Widths that are multiples of a week or a month stay in the pipeline, since TQL aligns them to the calendar, and so do widths below a second in SQL Server.

*By @mavam.*

### Truncated Log Analytics results warn instead of failing

When a query result exceeds the Log Analytics record count or data size limit, by default 500,000 rows or 64 MiB, the service returns the rows it kept together with a partial error. `from_azure_log_analytics` now emits these rows with a single warning instead of failing the pipeline:

```
warning: the result is incomplete
  = note: Log Analytics truncates a result beyond its record count or data size limit, by default 500,000 rows or 64 MiB
  = hint: narrow the query or its time range, return fewer columns, or limit or aggregate rows in the query, for example with `take` or `summarize`
```

Other partial errors, such as overflows or queries that exceed their memory budget, still fail the pipeline because their rows may be incorrect.

Results with more than about two million values no longer fail with "result exceeds the decoded value limit". Every result within the service's limits now decodes.

*By @mavam.*

## 🐞 Bug fixes

### Accurate CPU times, start times, and memory sizes from processes on Linux

On Linux, the `processes` operator reported `utime` and `stime` in whole seconds, which showed a mostly idle process as using no CPU at all, and left `startup` empty. It also reported `peak_mem` and `swap` in kilobytes rather than bytes, unlike `rsize` and `vsize`. The operator now reports CPU times to the millisecond, the time each process started, and all memory sizes in bytes.

*By @mavam.*

### Package hints name the tenzir command

When a workspace's lock is unreadable, misses a dependency, or names a package that is not fetched yet, the hint now tells you to run `tenzir package lock` or `tenzir package fetch` instead of the `tenzir-platform` variants, matching the unified `tenzir` command of Tenzir 7.

*By @mavam.*

### Parallelism max respects container CPU limits

A degree of parallelism of `max` now respects the CPU limit of a container. Previously, it used the number of CPUs of the host, so a container limited to 2 CPUs on a 64-core host ran 64 instances of every parallelizable operator. Now `max` is the number of CPUs that the process may run on, capped by the CPU quota of its cgroup and rounded up.

*By @jachris.*

### Platform connection drops behind load balancers and under load

Nodes no longer lose their connection to the Tenzir Platform every few minutes when a load balancer or proxy with an idle timeout sits between the two. The node now sends a WebSocket ping every 20 seconds. Previously, it could stay silent for a full minute, so a load balancer with a 60-second idle timeout dropped the connection again and again, and the node logged `platform plugin received an unexpected error: End of file`.

Nodes also keep the connection while they answer many requests at once, for example when a dashboard loads. Previously, this could corrupt the encrypted connection. The node then logged `unexpected error: unspecified system error` or `stream truncated` and briefly appeared as disconnected in the app.

*By @tobim.*

### Zeek TSV round trip with write_zeek_tsv and read_zeek_tsv

The output of `write_zeek_tsv` now reads back with `read_zeek_tsv`. Previously, reading a log with a `time` or `interval` column failed, because `write_zeek_tsv` did not write these values as Zeek does. It now writes times and intervals as seconds, as in `1794812400.123456`, omits the `zeek.` prefix of the schema name in `#path`, and ends the log with a `#close` line.

Numbers that round up to the next whole number, such as `0.99999999996`, no longer print as `0.1` in `write_lines`, and intervals such as `0.9999996s` no longer print as `0.100000` in `write_zeek_tsv`.

*By @mavam.*
