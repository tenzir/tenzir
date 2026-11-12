This release adds lossless OCSF compaction with ocsf_compact, computes category_uid and type_uid in ocsf_derive, and updates the bundled OCSF schemas. Operators that follow Splunk, ClickHouse, MySQL, SQL Server, KQL, and SentinelOne sources now use the native semantics of those systems. The release also adds Redis source and sink operators and lowers the memory and CPU usage of nodes.

## 🚀 Features

### Computed category_uid and type_uid in ocsf_derive

The `ocsf_derive` operator and function now compute missing `category_uid` and `type_uid` fields, so OCSF mappings no longer need to set them:

```tql
from {class_uid: 1001, metadata: {version: "1.5.0"}, activity_id: 1}
ocsf_derive
```

```tql
{
  class_uid: 1001,
  metadata: {
    version: "1.5.0",
  },
  activity_id: 1,
  category_uid: 1,
  type_uid: 100101,
  activity_name: "Create",
  category_name: "System Activity",
  class_name: "File System Activity",
  type_name: "File System Activity: Create",
}
```

The `category_uid` follows from the class, including extension classes. The `type_uid` is `class_uid * 100 + activity_id`, with `activity_name` as a fallback when `activity_id` is missing. Existing values stay, and `ocsf_derive` warns when a `type_uid` does not match `class_uid` and `activity_id`.

*By @zedoraps.*

### Lossless OCSF compaction with ocsf_compact

The new `ocsf_compact` function removes OCSF enum strings that `ocsf_derive` can rebuild from their integer counterparts. Use it to store only the ids and restore the names when reading the data:

```tql
this = ocsf_compact(this)
```

For example, `{activity_id: 1, activity_name: "Logon", severity_id: 1, severity: "Informational"}` becomes `{activity_id: 1, severity_id: 1}`. The function works on nested records and enum lists, such as `answers[].flags`.

The compaction is lossless: running `ocsf_derive` afterward restores the original event. To make this possible, the function keeps every string that `ocsf_derive` cannot rebuild exactly. This includes the data source specific value of an `Other` (`99`) enum, values that do not match their integer, unknown integers, and strings without an integer.

With `uids=true`, the function also removes `category_uid` and `type_uid` where `ocsf_derive` computes them from `class_uid` and `activity_id`:

```tql
this = ocsf_compact(this, uids=true)
```

Both fields are required in OCSF, so the events are only valid OCSF again after `ocsf_derive` restored them.

*By @zedoraps.*

### Redis source and sink operators

The new `from_redis` and `to_redis` operators read events from and write events to Redis. Pipelines can now consume and fill work queues in Redis lists, subscribe and publish to pub/sub channels, and keep state in Redis keys.

The `from_redis` operator pops items from one or more lists with a blocking pop and emits one event per item, with the item in the `message` field. By default, it pops from the right end, which consumes a list in FIFO order when producers push to the left end:

```tql
from_redis "events", url="redis://redis.example.org:6379"
this = string(message).parse_json()
```

With `command="subscribe"` or `command="psubscribe"`, the operator receives messages from pub/sub channels instead. The `metadata_field` option adds the list key, channel, and pattern to every event.

The `to_redis` operator writes one value per event. It pushes to the left end of a list by default and supports `rpush`, `publish`, `set`, `sadd`, `zadd`, and `hset` as well, with an optional `ttl` for the written keys:

```tql
to_redis "status:" + host, command="set", message=status, ttl=1h
```

Both operators accept a `redis://` or `rediss://` URL, a database index, a user and password as secrets, and TLS settings. They reconnect with a warning when the connection drops. When the pipeline stops, `from_redis` emits the items of a pop in flight, and a cancelled pipeline hands the items that it popped but did not emit back to their list.

*By @mavam.*

### The `version` function

The new `version()` function returns the same record as the `version` operator, so pipelines can use version information in expressions:

```tql
from {x: 1}
tenzir = version().version
```

*By @IyeOnline.*

### Time zone support for parse_time

The `parse_time` function has a new `timezone` option that interprets the parsed wall-clock time in an IANA time zone, such as `Europe/Zurich`, and returns the corresponding UTC time. Daylight saving time is taken into account.

This is useful for RFC 3164 syslog timestamps, which carry the sender's local time without a year or time zone. Because `timezone` accepts any expression, you can resolve the zone per event, for example from an asset inventory:

```tql
timestamp = timestamp.parse_time("%b %e %H:%M:%S", reference=now(), timezone=host.timezone)
```

Local times that a daylight saving switch repeats resolve to the earlier instant, and local times that it skips shift forward by the length of the gap. An explicit UTC offset in the input, parsed with `%z`, takes precedence over `timezone`. Unknown time zone names produce a warning and a null result, and a null `timezone` keeps the previous behavior of reading the time as UTC.

*By @mavam.*

## 🔧 Changes

### Automatic syslog framing detection

The `read_syslog` operator now automatically detects octet-counted and newline-delimited messages. A single TCP listener can receive both framing methods from different clients:

```tql
accept_tcp "0.0.0.0:514" {
  read_syslog
}
```

Framing is selected independently for each connection. Existing pipelines that set `octet_counting=true` or `octet_counting=false` keep their behavior and emit a deprecation warning. Omit the parameter to enable automatic detection.

Automatic detection requires a positive decimal length, one ASCII space, and a complete syslog priority header (`<PRI>`). Other input uses newline framing. Use `octet_counting=true` for counted streams whose first message has no PRI header.

*By @zedoraps.*

### Latest upstream OCSF schemas

Tenzir tracks the OCSF schemas upstream and now bundles their latest definitions, with full support for OCSF `1.9.0` and all earlier stable versions, plus preview support for OCSF `1.10.0-dev`.

*By @tenzir-bot.*

### Lower base memory usage of pipelines

We have roughly halved the idle memory usage of operators they would take even while not processing any data.

*By @IyeOnline.*

### Lower idle CPU usage of nodes

Idle nodes now use considerably less CPU.

*By @IyeOnline.*

### Native ClickHouse semantics for operators after from_clickhouse

`from_clickhouse` now hands ClickHouse the operators that follow it in a `table` read, as far as ClickHouse can spell them: `where`, `select`, `drop`, `summarize` with `count`, `min`, and `max`, `sort`, `head`, and `top` or `rare` followed by `head`, including the operators after a `summarize`. Those operators leave the pipeline and run in ClickHouse with ClickHouse's semantics. For example, a comparison with `NULL` yields `NULL`, so `not (n == 1)` and `s != "a"` no longer keep rows where the field is `null`, and `null` sorts last:

```tql
from_clickhouse table="events", host="clickhouse.example.com"
where severity >= 3
summarize src_ip, events=count()
sort -events
head 10
```

Queries for the latest events, such as `sort -time | head 100`, also run in ClickHouse, which then returns only the rows that the pipeline keeps.

A query that ClickHouse rejects fails the pipeline, for example when it compares a column with a value of another type, such as a string column with an IP address, or names a column that the table lacks. Fields below a `JSON` column compare in ClickHouse only where all their values share a type. Where such a field holds an object, `x != null` holds and `x == null` does not, but a comparison with a value, such as `x == 5`, never matches. ClickHouse keeps no trace of an empty object, which reads like a missing field. A sort by such a field fails the read if a later operator needs its order, as in `sort x | head 1 | sort y`. A field `a.b` names the element `b` of the tuple `a`, while `this["a.b"]` names the column `a.b`.

For a table with a catch-all column, ClickHouse runs `where`, `sort`, and `head` on the fields of the reconstructed events, as long as each field comes from a single column. A `select`, `drop`, or `summarize` on such a table fails the read before it runs any query, and so does a field that spans several columns, an `ALIAS` or `MATERIALIZED` column, or a column that `from_clickhouse` cannot read. The error suggests `pushdown=false`.

A `table` read now fails instead of returning a partial result when it exceeds a limit of the session, such as `max_result_rows` with `result_overflow_mode = 'break'`, also with `pushdown=false`. A read with `sql` keeps the session's settings.

Stopping a pipeline now also stops a `from_clickhouse` or `from_hydrolix` read whose query is still sorting or summarizing on the server, instead of waiting until the server sends its first result.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_clickhouse table="events", host="clickhouse.example.com", pushdown=false
where not (severity == 3)
```

`from_hydrolix` keeps its previous behavior.

*By @mavam and @zedoraps.*

### Native KQL semantics for operators after from_microsoft_defender and from_azure_log_analytics

`from_microsoft_defender` and `from_azure_log_analytics` now hand the service the operators that follow them, as long as KQL can spell them: `where`, `select`, `drop`, `summarize` with `count()`, `count(x)`, `min`, `max`, and `sum`, `sort`, and `head`. The operators append them to your query as `where`, `project`, `project-away`, `summarize`, `sort`, and `take` stages, where they leave the pipeline and run with KQL's semantics:

```tql
from_microsoft_defender "DeviceNetworkEvents",
  azure_auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("defender-client-secret"),
  }
where RemotePort == 443
summarize DeviceName, connections=count()
sort -connections
head 10
```

- **Filters:** strings compare case-sensitively, and a string is never `null` in KQL but may be empty. IP addresses compare as text, and subnets match with `ipv4_is_in_range` or `ipv6_is_in_range`. A comparison of mismatched types either holds for no row or fails the query.
- **Fields:** a field that the query does not return fails the query, as does a type that KQL rejects, such as a sum over strings or a `summarize` by a `dynamic` column.
- **Summaries:** `summarize` keeps a group for `null` keys, and `count(x)` counts the values that are not empty.
- **Sorts:** `sort` puts `null` first in ascending order and last in descending order.

Before, the operators only preselected rows in the service and filtered them again in Tenzir. A query that the service rejects now fails the pipeline. After a query whose result is not a single table, such as a query with several tabular statements, or one with `render`, `fork`, or `facet`, the operators stay in the pipeline.

`from_microsoft_defender` now warns when a result has 100,000 rows, the most that advanced hunting returns, since the service may have truncated it without telling.

Use `pushdown=false` to send your query unchanged and keep every operator in the pipeline with TQL semantics:

```tql
from_azure_log_analytics "SecurityEvent",
  workspace_id=secret("log-analytics-workspace"),
  azure_auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("log-analytics-client-secret"),
  },
  pushdown=false
where Account == "alice"
```

*By @mavam.*

### Native MySQL semantics for operators after from_mysql

`from_mysql` now hands MySQL the operators that follow it in a `table` read, as far as MySQL can spell them: `where`, `select`, `summarize`, `sort`, `head`, `top` or `rare` followed by `head`, and a `drop` after a `select`. Those operators leave the pipeline and run in MySQL with MySQL's semantics. Text compares, groups, and sorts under the collation of its column, which often ignores case and accents. `NULL` sorts first, and `name != "alpha"` no longer keeps rows where `name` is `NULL`:

```tql
from_mysql table="users", host="db.example.com", database="app"
where name == "alice" and created > 2025-01-01
select name, created
sort -created
head 10
```

MySQL can now use its indexes for these filters. Times compare in UTC: a `table` read sets the session's time zone to UTC, so `TIMESTAMP` columns also arrive in UTC. A `live` read hands MySQL its filters only. After a restore, a `table` read runs its query again and skips the events that it emitted before, which assumes that MySQL returns the rows in the same order, as a `sort` guarantees.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_mysql table="users", host="db.example.com", database="app", pushdown=false
where name == "alice"
```

*By @mavam.*

### Native SentinelOne semantics for operators after from_sentinelone_data_lake

`from_sentinelone_data_lake` now hands SentinelOne the operators that follow it, as long as PowerQuery can spell them: `where`, `select`, `summarize` with `count()` and `count(x)`, `sort` after a `select` or `summarize`, `head`, and `top` or `rare`. Those operators leave the pipeline and run in SentinelOne with SentinelOne's semantics:

```tql
from_sentinelone_data_lake "https://example.sentinelone.net",
  token=secret("sentinelone-token"),
  start=now() - 1h
where event.type == "DNS Resolved"
summarize endpoint.name, lookups=count()
sort -lookups
head 10
```

- **Filters:** `where` becomes a `filter` that compares case-sensitively. `!=` also keeps events without the field, numbers compare as doubles, and before a `summarize`, a numeric string may compare equal to a number. Filters on `timestamp` also narrow the request window.
- **Reads:** filters and a `head` on whole events become a LOG query, which pages through complete events without a cap. All other reads become a PowerQuery, which now ends in `| limit 1600000` unless it ends in a `head`, so that SentinelOne's default cap of 1,000 rows no longer applies.
- **Summaries:** SentinelOne groups events without the field under `null`, and sorts `null` first. A field that exists only as nested fields, such as `endpoint` for `endpoint.name`, is `null` in SentinelOne. Counts are integers also with `raw=true`.
- **Queries:** the operators after `query` now also run in SentinelOne, appended to your query, and see its result. A `query` without operators after it goes out unchanged, with SentinelOne's default cap. If your query ends in `|` or inside a comment or string, the operators stay in the pipeline.

Before, `from_sentinelone_data_lake` only preselected events in SentinelOne and filtered them again in Tenzir, and computed summaries in SentinelOne only where it could verify the result, with further queries otherwise. It now sends a single query. A query that SentinelOne rejects fails the pipeline. Results that SentinelOne reports as incomplete, such as the default cap of 1,000 rows or omitted events, now produce a warning instead of an error, as does a PowerQuery result with 1,600,000 rows.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_sentinelone_data_lake "https://example.sentinelone.net",
  token=secret("sentinelone-token"),
  start=now() - 1h,
  pushdown=false
where event.type == "DNS Resolved"
```

*By @mavam.*

### Native Splunk semantics for operators after from_splunk

`from_splunk` now hands Splunk the operators that follow it, as long as SPL can spell them: `where`, `select`, `drop`, `summarize`, `sort`, `head`, and `top` or `rare` followed by `head`. Those operators leave the pipeline and run in Splunk with Splunk's semantics:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where sourcetype == "linux_secure" and user != "root"
summarize user, attempts=count()
where attempts > 10
sort -attempts
head 20
```

- **Filters:** with `index`, filters right after the operator extend the base `search` command where it can spell them, such as `sourcetype == "linux_secure"`, so they match without regard to case, and networks match by CIDR. All other filters become `where` stages, which compare text exactly, drop events without the field for `!=`, and hold for a multivalue field if any of its values matches. Filters on `_time` also narrow `earliest` and `latest` when these are absolute.
- **Summaries:** `summarize` becomes `stats`, also for filters, sorts, and limits after it. Splunk drops events whose group key is `null`, puts an event with several values into the group of each value, and counts each value in `count(field)`. Counts are integers also with `raw=true`.
- **Sorts and limits:** `sort` keeps exactly as many events as the `head` after it, or all of them, rather than Splunk's default of 10,000.

Before, `from_splunk` only preselected events in Splunk and filtered them again in Tenzir, and it computed summaries in Splunk only where the result matched `summarize`, with a second search otherwise. It now sends a single search. A search that fails in a `stats` stage, which Splunk reports as an empty result, now fails the pipeline.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics. The search still returns only the fields that the pipeline needs:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")},
  pushdown=false
where user != "root"
```

With `options`, or after a search with `fieldformat` or a macro, the operators stay in the pipeline. After a search with a command that has side effects, such as `outputlookup`, a `head` stays in the pipeline.

*By @mavam.*

### Native SQL Server semantics for operators after from_microsoft_sql

`from_microsoft_sql` now hands SQL Server the operators that follow it in a `table` read, as far as T-SQL can spell them: `where`, `select`, `summarize`, `sort`, `head`, `top` or `rare` followed by `head`, and a `drop` after a `select`. Those operators leave the pipeline and run in SQL Server with its semantics. Text compares, groups, and sorts under the collation of its column, which often ignores case and trailing spaces. `NULL` sorts first, and `name != "alpha"` no longer keeps rows where `name` is `NULL`:

```tql
from_microsoft_sql table="dbo.logins", host="db.example.com", database="audit"
where user == "admin" and ts > 2025-01-01
select user, ts
sort -ts
head 10
```

SQL Server can now use its indexes for these filters. Times compare as instants in UTC with every temporal type. T-SQL has no boolean values, so a bare `where flag` on a `bit` column stays in the pipeline, while `where flag == true` goes into the query. A `live` read hands SQL Server its filters only. The operator takes over operators before it knows the types of the columns, so a query that SQL Server rejects fails the pipeline, for example `max` of a `bit` column or a comparison of a legacy `text` column; use `pushdown=false` for such pipelines.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_microsoft_sql table="dbo.logins", host="db.example.com", database="audit", pushdown=false
where user == "admin"
```

*By @mavam.*

### OCSF operator deprecations

The `ocsf_cast`, `ocsf_trim`, and `ocsf_derive` operators are deprecated. Use their function counterparts for both top-level and nested OCSF events. Existing operator calls continue to work and emit a warning with a migration hint. To transform the whole event, assign the function result to `this`:

```tql
this = ocsf_cast(this)
this = ocsf_derive(this)
this = ocsf_trim(this)
```

Pass operator options as named function arguments, for example `this = ocsf_cast(this, null_fill=true)`. The functions return `null` for unresolved OCSF events instead of dropping them. The `ocsf_cast` function also validates required fields and constraints, and does not assign `@name`.

*By @zedoraps.*

## 🐞 Bug fixes

### Azure token requests over HTTPS and readable certificate errors

`from_microsoft_defender`, `from_microsoft_graph`, `from_microsoft_365_activity`, and `from_azure_log_analytics` can fetch Microsoft Entra ID access tokens over HTTPS again. Since v6.20.0, every token request failed with `certificate verify failed`, because the token endpoint was verified against the host name of the API server.

The `tls` options of these operators and the node's `tls` configuration now apply to the token endpoint as follows: `cacert`, `skip_peer_verification`, `min_version`, and `ciphers` apply, but `certfile` and `keyfile` do not, so the token endpoint never receives the client certificate. Likewise, when `from_http` follows pagination links to another host, it no longer sends the client certificate there.

HTTP clients now also name the reason when a server certificate fails verification and stop retrying:

```
error: failed to fetch Azure access token: TLS certificate verification failed for `login.microsoftonline.com`: unable to get local issuer certificate
```

Previously, the error only read `SSL routines::certificate verify failed`.

Connections to HTTPS proxies now also trust the CA bundle that the `SSL_CERT_FILE` environment variable names, like all other HTTPS connections, instead of only the system's default bundles. When an HTTP client cannot set up TLS for its server or proxy, for example because no CA bundle exists, or cannot resolve the host, operators now report an error instead of failing with `uncaught exception in pipeline`.

*By @mavam.*

### Bounded memory in accept_tcp with short-lived connections

The `accept_tcp` and `accept_unix_socket` operators no longer use unbounded memory when clients open a new connection per message and the rest of the pipeline cannot keep up. Previously, the operators freed a `max_connections` slot as soon as a client closed its connection, even though the subpipeline for that connection was still waiting to pass on its events. These waiting subpipelines could pile up without limit, which caused out-of-memory kills.

Now, a connection holds its slot until its subpipeline finishes. When the pipeline falls behind, the operators stop accepting new connections, and clients wait in the listen backlog until a slot becomes free.

*By @aljazerzen.*

### Byte metrics no longer count filtered rows

Operator byte metrics in the new executor now count only the events that an upstream filter lets through. Previously, they included the bytes of events that the filter had already dropped:

```tql
subscribe "events"
where severity == "high"
discard
```

Here, `discard` now reports the bytes of the high-severity events only.

*By @IyeOnline.*

### Filters reach the source through constant literals again

Filters on OCSF fields reach the source again when they follow an OCSF mapping that assigns constant lists or records, such as `metadata = {profiles: ["cloud"], version: "1.9.0"}`. Since v6.21.0, such filters stayed behind the assignment, because the optimizer treated every read of a constant list or record as a copy of computed work. Such literals now evaluate to a constant without per-event work, so filters may read them any number of times.

*By @jachris.*

### Imported events keep the exact registered schema

The `import` operator now keeps hidden attributes from the old data model on events with a registered schema, such as `suricata.flow`. Before, these events were stored under a second schema next to the one from older versions.

*By @IyeOnline.*

### Lower CPU usage for publish metrics

The `publish` operator uses less CPU when collecting per-schema metrics. Its `schema_id` values now match those reported by `measure events` and `type_id(this)` instead of legacy schema fingerprints.

*By @aljazerzen.*

### No false warnings for custom type_name values in ocsf_derive

The `ocsf_derive` operator and function no longer warn about an invalid `type_name` when the `type_uid` denotes an "Other" activity, such as `300299` with `type_name: "Authentication: user_login"`. Like other "Other" values, the name carries a data source specific value.

*By @zedoraps.*

### Pipelines with `if` or `match` before `head` end

A pipeline now ends once `head`, `slice`, or `taste` has what it needs, even if an `if` or a `match` comes between them and the source. Before, the operators upstream of the `if` or `match` were never told to stop, so a source that keeps producing, such as `every` or `subscribe`, kept the pipeline running after its last event:

```tql
every 1h {
  from {x: 1}, {x: 2}, {x: 3}
}
if x > 1 {
  y = 1
}
head 2
```

This pipeline printed two events and then never ended.

*By @zedoraps.*
