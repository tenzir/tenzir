Tenzir now parallelizes Kafka operators and HTTP-based sinks while pushing more filters, projections, and limits toward data sources, so pipelines process less data and move it faster. This release also adds KQL query sources for Microsoft Defender advanced hunting and Azure Log Analytics, expands ClickHouse pushdown, and improves the reliability of Kafka, Google Cloud Pub/Sub, and Base64 processing.

## 🚀 Features

### Azure Log Analytics query source

The new experimental `from_azure_log_analytics` operator runs KQL queries against an Azure Log Analytics workspace and emits typed result rows. Use it to read workspace data, including tables used by Microsoft Sentinel, into a Tenzir pipeline:

```tql
from_azure_log_analytics "SecurityEvent | project TimeGenerated, Computer, EventID",
  workspace_id=secret("azure-workspace-id"),
  azure_auth={
    tenant_id: secret("azure-tenant-id"),
    client_id: secret("azure-client-id"),
    client_secret: secret("azure-client-secret"),
  },
  start=2026-01-01,
  end=2026-01-02
```

The operator supports app-only Entra authentication, optional paired time bounds, and configurable query timeouts. It validates the full response before emitting rows, preserves exact decimal values as strings, retries transient failures, and rejects service-reported partial results. It does not paginate or poll continuously; restarting reruns the query. Grant the application workspace query permissions, which are separate from ingestion permissions.

*By @mavam.*

### Microsoft Defender advanced hunting source

The new `from_microsoft_defender` operator runs arbitrary KQL queries against Microsoft Defender advanced hunting through Microsoft Graph and emits typed result rows. It supports app-only Entra authentication and optional paired `start` and `end` timestamps.

```tql
from_microsoft_defender "DeviceEvents | take 10",
  azure_auth={
    tenant_id: secret("AZURE_TENANT_ID"),
    client_id: secret("AZURE_CLIENT_ID"),
    client_secret: secret("AZURE_CLIENT_SECRET"),
  }
```

Grant the Microsoft Graph application permission `ThreatHunting.Read.All` with administrator consent. This finite source validates the full response before emitting rows, retries transient failures, and fails on service errors. It does not paginate or maintain a cursor; restarting reruns the query.

*By @mavam.*

### Parallel requests for HTTP-based sinks

The `to_splunk`, `to_opensearch`, `to_elasticsearch`, and `to_azure_log_analytics` operators now send several requests at the same time instead of waiting for each response before sending the next batch. The new `parallel` option bounds how many requests one operator instance keeps in flight:

```tql
to_splunk "https://splunk.example.org:8088", hec_token=secret("hec"), parallel=8
```

It defaults to `8`. Set `parallel=1` to restore the previous behavior of sending one request at a time. The `to_google_secops` operator, which already had a `parallel` option, now uses the same default of `8` instead of `50`.

These sinks are also parallelizable now, so pipelines that run with parallelism enabled replicate them across instances. Every instance buffers, authenticates, and sends on its own, which makes the pipeline-wide number of in-flight requests `parallel` times the degree of parallelism.

Both mechanisms give up the guarantee that the destination receives requests in the order the sink produced them.

*By @aljazerzen.*

### Parallelize the Kafka operators

`from_kafka` and `to_kafka` now run as multiple instances when the pipeline is parallelized.

Both stay sequential where replication would change results: `from_kafka` with `count`, with an `offset` other than `stored`, or with `group.instance.id` set, and `to_kafka` with `key`.

*By @aljazerzen.*

### Time, IP, and function pushdown in `from_clickhouse`

The `from_clickhouse` operator now translates many more of the predicates that follow it into the `SELECT` it sends to ClickHouse. Beyond numbers, strings, and booleans, the pushdown covers:

- `time` values against `Date`, `Date32`, `DateTime`, and `DateTime64` columns, including columns in a non-UTC time zone.
- `ip` values against `IPv4` and `IPv6` columns, and `in` with a subnet.
- Equality on enum names, UUIDs, and `FixedString` values, and ordering on strings.
- Comparisons between two columns of the same kind.
- The `starts_with`, `ends_with`, and `length_bytes` functions, and substring search with `"needle" in haystack`.
- Arithmetic that cannot overflow, such as `port + 1` or `bytes / 1024`, and expressions of literals such as `2024-01-01 + 1d`.

A `select` of a nested field such as `meta.level` now transfers only that element of the tuple instead of the whole column. For example,

```tql
from_clickhouse table="logs.events"
where ts > 2024-01-01 and src_ip in 10.0.0.0/8 and status == "high"
select id, message, meta.level
head 100
```

sends a query equivalent to:

```sql
SELECT id, message, CAST(tuple(meta.level), 'Tuple(level Int64)') AS meta,
       ts, src_ip, status
FROM logs.events
WHERE ts > toDateTime('2024-01-01 00:00:00', 'UTC')
  AND src_ip BETWEEN toIPv4('10.0.0.0') AND toIPv4('10.255.255.255')
  AND status = 'high'
LIMIT 100
```

Tables that store IP addresses as `String` now work with `ip` literals in `table` mode. Comparing a string with an `ip` is a type mismatch in TQL, so `where src == 1.1.1.1` used to match no rows against such a column. The operator now adapts the predicate to the column's type: `src == 1.1.1.1` and `src in [1.1.1.1, ::1]` compare against the canonical text of the address and are pushed as plain string comparisons, and `src in 10.0.0.0/8` parses the column in Tenzir behind a prefilter that lets ClickHouse drop the rows it can rule out.

Every pushed predicate yields the same rows as its local evaluation, including for `null` values and for values that ClickHouse can store but Tenzir cannot represent, such as dates past the year 2262. Anything without an exact translation keeps running locally with unchanged results.

*By @mavam.*

## 🔧 Changes

### Broader pipeline pushdown through transforms

Pipeline optimization now carries field selection and row limits through more common transformations, allowing sources and readers to materialize less data in pipelines that use field removal, batching, replacement, history, time shifting, enrichment, and OCSF processing.

Conservative barriers preserve event ordering, history, diagnostics, and whole-event observations where pushdown would change behavior. Enrichment and lookup operators pass filters and limits through, so optimization may skip context requests and DNS lookups for events that downstream discards.

*By @aljazerzen.*

### Projection pushdown through nested pipelines

Tenzir now pushes field projections through branching operators and nested pipelines. Pipelines using `if`, `match`, `fork`, `fork_merge`, `merge`, `parallel`, `group`, `window`, `every`, `load_balance`, and `strict` can avoid reading fields that neither the main path nor any nested path needs, while retaining routing keys and other control-flow dependencies.

*By @aljazerzen.*

## 🐞 Bug fixes

### `from_kafka` no longer stops committing offsets after one bad partition

`from_kafka` committed the offsets of all its partitions in one request and kept every offset when that request failed. A single partition that could not be committed — because a rebalance moved it to another consumer — therefore failed every later commit too, so the operator stopped recording progress for the whole topic and re-read everything it had already delivered after a restart.

The operator now reads Kafka's per-partition commit results: partitions that committed are retired even when another partition in the same request failed, transient failures are retried on their own, and an offset that this consumer can never commit is dropped once with a warning. Uncommitted offsets of partitions that a rebalance takes away are dropped as well, since only their new owner can commit them.

It also no longer submits an offset for a partition it does not currently own. Kafka accepts such a commit under the committing member's own generation, so it overwrote the progress of the consumer that had taken the partition over, making that consumer re-read or skip records.

*By @aljazerzen.*

### `from_kafka` with `exit` no longer stops early on a rebalance

`from_kafka ..., exit=true` stopped as soon as it observed an empty partition assignment. Kafka's default rebalance protocol revokes every partition before assigning the new set, so any rebalance — triggered whenever another consumer joins or leaves the group — briefly left the operator without partitions and ended the run before the topic was read to its end.

The operator now distinguishes the two halves of a rebalance and only concludes from an assignment, never from a revocation — and only while that assignment is still the current one, so a rebalance that happens between observing an assignment and acting on it cannot end the run either. A genuinely empty assignment, such as when a parallel pipeline runs more instances than the topic has partitions, still finishes immediately.

*By @aljazerzen.*

### Exact field paths in pushed-down filters

Pushed-down filters now match complete field paths instead of matching fields by their suffix. For example, `where flow.pkts_toserver == 0` no longer matches `flow.bypassed.pkts_toserver`. This prevents unrelated nested fields from causing events to be incorrectly included or excluded.

Full schema-name prefixes remain supported for existing concept targets. After the prefix, the remaining field path must match exactly.

*By @tobim.*

### Google Cloud Pub/Sub sources stay running

Pipelines using `from_google_cloud_pubsub` no longer complete immediately after startup. The operator now keeps its subscription connection alive to continue receiving messages.

*By @raxyte.*

### Robust Base64 decoding

Tenzir now handles edge cases in Base64-encoded field values without crashing the node, allowing affected pipelines to continue processing.

*By @zedoraps.*
