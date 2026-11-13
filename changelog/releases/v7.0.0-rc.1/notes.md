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

### Sink byte metrics count the delivered payload

Sink operators now report the bytes they write consistently: the payload before any compression they apply, counted once it was delivered, and once however many retries it took. Together with sources, which report the bytes they read, this makes the bytes going into and out of a pipeline comparable.

For example, `to_splunk`, `to_hydrolix`, `to_opensearch`, `to_prometheus`, `to_google_secops`, `to_azure_log_analytics`, and `to_sentinelone_data_lake` now count the request body before compressing it, and only once the destination accepted it. `to_google_secops` counts nothing in `dry_run` mode. `to_google_cloud_logging` counts entries once Cloud Logging accepted them, `to_http` with `buffer_all` counts the body once the request succeeded, and `to_kafka` counts a message once the brokers acknowledged it.

*By @mavam.*
