Tenzir v6 ships with a rewritten execution engine that unlocks faster, more capable, and more scalable pipelines. Refer to the migration guide at https://docs.tenzir.com/guides/tenzir-v6-migration before upgrading.

## 💥 Breaking changes

### `$file` let-binding for filesystem readers

The filesystem and cloud object reader operators (`from_file`, `from_s3`, `from_azure_blob_storage`, `from_google_cloud_storage`) no longer accept the `path_field` option. Instead, the parsing subpipeline now has access to a `$file` let-binding describing the source file:

| Field   | Type     | Description                              |
| :------ | :------- | :--------------------------------------- |
| `path`  | `string` | The absolute path of the file being read |
| `mtime` | `time`   | The last modification time of the file   |

To attach the source path to each event:

```tql
// Before:
from_file "/data/*.json", path_field=source

// After:
from_file "/data/*.json" {
  read_json
  source = $file.path
}
```

This makes per-file metadata available throughout the parsing subpipeline rather than only on emitted events.

*By @raxyte in #6001.*

### `from_http` is now a pure HTTP client

The `from_http` operator no longer doubles as an HTTP server. It is now a pure HTTP client that issues one request and returns the response.

For accepting incoming HTTP requests, use the dedicated `accept_http` operator instead:

```tql
// Before:
from_http "0.0.0.0:8080", server=true { read_json }

// After:
accept_http "0.0.0.0:8080" { read_json }
```

In the parsing subpipeline, the response metadata is now exposed as the `$response` let-binding instead of being written into a `metadata_field`:

```tql
from_http "https://api.example.com/status" {
  read_json
  status_code = $response.code
  server = $response.headers.Server
}
```

Additionally, the `url` and `headers` arguments are now resolved as [secrets](https://docs.tenzir.com/explanations/secrets), so you can pass secret names instead of hardcoding tokens or sensitive URLs.

*By @aljazerzen in #5953.*

### `to_kafka` defaults to NDJSON-encoded messages

The default `message` expression of the `to_kafka` operator is now `this.print_ndjson()` instead of `this.print_json()`. Kafka messages are single-line records by default, so each event is now emitted as a single NDJSON line:

```json
{"timestamp":"2024-03-15T10:30:00.000000","source_ip":"192.168.1.100","alert_type":"brute_force"}
```

instead of pretty-printed multi-line JSON.

To restore the previous behavior, pass `message=this.print_json()` explicitly.

*By @lava in #5742.*

### `yara` requires finite input

The `yara` operator no longer accepts the `blockwise` argument. Instead, it buffers the entire input as one contiguous byte sequence and runs the YARA scan when the input ends. Matches can therefore span chunk boundaries, but `yara` is now only suitable for finite byte streams. Don't use it on never-ending inputs.

The `rule` argument now also accepts a single string in addition to a list of strings:

```tql
from_file "evil.exe", mmap=true {
  yara "rule.yara"
}
```

Removed:

```tql
yara ["rule.yara"], blockwise=true
```

*By @mavam in #6035.*

### Dedicated FTP source and sink operators

Two new operators provide first-class FTP and FTPS support with parsing and printing subpipelines:

- `from_ftp` downloads bytes from an FTP or FTPS server and forwards them to the parsing subpipeline.
- `to_ftp` uploads bytes produced by the printing subpipeline to an FTP or FTPS server.

```tql
from_ftp "ftp://user:pass@ftp.example.org/path/to/file.ndjson" {
  read_ndjson
}
```

```tql
to_ftp "ftp://user:pass@ftp.example.org/a/b/c/events.ndjson" {
  write_ndjson
}
```

The `load_ftp` and `save_ftp` operators have been removed, and the `ftp://` and `ftps://` URL schemes no longer dispatch via `from` and `to`. Use `from_ftp` and `to_ftp` directly.

*By @mavam in #6044.*

### OpenSearch ingestion with `accept_opensearch`

The new `accept_opensearch` operator starts an OpenSearch-compatible HTTP server and turns incoming Bulk API requests into events:

```tql
accept_opensearch "0.0.0.0:9200"
publish "events"
```

The operator buffers each bulk request body up to `max_request_size`, optionally decompresses it based on the `Content-Encoding` header, parses the NDJSON payload, and emits the resulting records. Set `keep_actions=true` to also keep the OpenSearch action objects (e.g., `{"create": ...}`) in the stream.

The `from_opensearch` operator has been removed. Use `accept_opensearch` instead. The `elasticsearch://` and `opensearch://` URL schemes now dispatch to `accept_opensearch` via `from`.

*By @aljazerzen in #6066.*

### Removed `real_time` argument from `measure`

The `measure` operator no longer accepts the `real_time` argument. The operator's emission cadence is now governed entirely by the executor's backpressure, so the option no longer has a meaningful effect.

Remove `real_time=true` or `real_time=false` from your pipelines:

```tql
// Before:
measure real_time=true

// After:
measure
```

*By @aljazerzen in #5880.*

### Renamed `from_gcs` to `from_google_cloud_storage`

The `from_gcs` operator has been renamed to `from_google_cloud_storage` so that its name matches the new `to_google_cloud_storage` writer:

```tql
// Before:
from_gcs "gs://my-bucket/data/**.json"

// After:
from_google_cloud_storage "gs://my-bucket/data/**.json"
```

Update test suites that reference `from_gcs` in `requires.operators` accordingly.

*By @raxyte in #5766.*

## 🚀 Features

### `from_http` infers response parsers

The `from_http` operator now accepts requests without an explicit parser subpipeline when Tenzir can infer the response format from the `Content-Type` header or URL extension:

```tql
from_http "https://example.com/events.json"
```

Explicit parser subpipelines continue to take precedence over inferred formats.

*By @mavam and @codex.*

### Add `auto_fill` option to `read_csv`, `read_tsv`, `read_ssv`, and `read_xsv`

The `read_csv`, `read_tsv`, `read_ssv`, and `read_xsv` operators now accept an `auto_fill=true` option. When set, the parser silently fills missing trailing columns with `null` instead of emitting a warning, which is useful when working with feeds that legitimately omit optional trailing fields.

*By @jachris and @claude.*

### CloudWatch Logs operators

Tenzir now supports reading from and writing to CloudWatch Logs with the new `from_amazon_cloudwatch` and `to_amazon_cloudwatch` operators. The source can subscribe to live streams with `mode="live"`, search historical log groups with `mode="search"`, or replay one stream with `mode="replay"`.

```tql
from_amazon_cloudwatch "/aws/lambda/api", mode="search", filter="ERROR"
```

The default sink can send events with `PutLogEvents`, including configurable batching, timestamp handling, parallel requests, and AWS IAM authentication via `aws_iam`. The sink can also write to the CloudWatch HTTP ingestion endpoints by setting `method` to `json`, `ndjson`, or `hlc`, with either SigV4 or bearer-token authentication.

```tql
to_amazon_cloudwatch "/tenzir/events",
  stream="default",
  payload=message,
  timestamp=ts
```

*By @mavam and @codex in #6180.*

### Dedicated TCP source and sink operators

Tenzir now has dedicated TCP source and sink operators that match the same client/server split as the HTTP operators:

- `from_tcp` connects to a remote TCP or TLS endpoint as a client.
- `accept_tcp` listens on a local endpoint and spawns a subpipeline per accepted connection. Inside the subpipeline, `$peer.ip` and `$peer.port` identify the connecting client; with `resolve_hostnames=true`, `$peer.hostname` is also available from reverse DNS.
- `to_tcp` connects to a remote endpoint and writes serialized bytes.
- `serve_tcp` listens for incoming connections and broadcasts pipeline output to all connected clients.

Each operator takes a parsing or printing subpipeline so connection management, framing, and serialization stay separate concerns:

```tql
accept_tcp "0.0.0.0:8090" {
  read_json
}
```

```tql
to_tcp "collector.example.com:5044" {
  write_json
}
```

`from_tcp` and `to_tcp` reconnect with exponential backoff on connection failure. All four operators support TLS via the `tls` option.

The legacy `load_tcp` and `save_tcp` operators are now deprecated. The `tcp://` and `tcps://` URL schemes still dispatch to them via `from` and `to`.

*By @mavam in #5744 and #6017.*

### DNS result caching in `dns_lookup`

The `dns_lookup` operator now caches DNS results and reuses them across lookups. Forward-lookup results gain a `ttl` field that shows the remaining lifetime of the cached answer:

```tql
from {host: "example.com"}
dns_lookup host
```

If Tenzir cannot initialize DNS resolution at all, the operator now emits an error and stops instead of writing `null` results for every event. Individual failed or timed-out lookups still produce `null`, as before.

*By @mavam in #6034.*

### Event throughput metrics for the new executor

Pipeline metrics now report event throughput alongside byte throughput for pipelines running on the new executor:

```tql
metrics "pipeline"
summarize ingress_events=sum(ingress.events), ingress_bytes=sum(ingress.bytes), egress_events=sum(egress.events), pipeline_id
sort -egress_events
```

This makes node metrics distinguish the amount of data transferred from the number of events processed.

*By @mavam and @codex.*

### from_amqp queue arguments

`from_amqp` now accepts a `queue_arguments` record for RabbitMQ queue declaration arguments:

```tql
from_amqp "amqp://broker/vhost",
          queue="events",
          queue_arguments={
            "x-queue-type": "quorum",
            "x-quorum-initial-group-size": 3
          }
```

Use this to declare queues with broker-specific settings such as quorum queues, maximum lengths, message TTLs, single active consumers, and dead-letter exchanges.

*By @mavam and @codex in #6139.*

### HEC metadata and raw endpoint support in `to_splunk`

The `to_splunk` operator gains three new options for richer HEC metadata.

Use `time=` to set the per-event Splunk timestamp from an expression that evaluates to a Tenzir `time` or a non-negative epoch in seconds:

```tql
from {message: "login succeeded", observed_at: 2026-04-24T08:30:00Z}
to_splunk "https://localhost:8088",
  hec_token=secret("splunk-hec-token"),
  time=observed_at
```

Use `fields=` to attach indexed HEC fields. The expression must evaluate to a flat record whose values are strings or lists of strings:

```tql
to_splunk "https://localhost:8088",
  hec_token=secret("splunk-hec-token"),
  event={message: message},
  fields={user: user, tags: tags}
```

Use `raw=` to send already-formatted text to the HEC raw endpoint (`/services/collector/raw`). The `raw` expression must evaluate to a `string`. Multiple events in one request are separated by newlines, and request-level metadata such as `host`, `source`, `sourcetype`, `index`, and `time` is sent as query parameters:

```tql
to_splunk "https://localhost:8088",
  hec_token=secret("splunk-hec-token"),
  raw=line,
  source=source,
  sourcetype="linux_secure"
```

`raw` and `event` are mutually exclusive; `fields` is not supported with `raw`.

*By @mavam in #6074.*

### HEC queue selection in `to_splunk`

The neo `to_splunk` implementation now accepts `queue="indexing"` and `queue="typing"` for selecting the Splunk HEC processing queue. The default `indexing` path keeps Splunk's regular HEC behavior, while `typing` sends the Splunk `parsingQueue` hint in HEC event envelopes for receivers that support this non-standard HEC metadata.

The default is `queue="indexing"`. The `typing` queue is rejected with `raw=...`, because Splunk's raw HEC endpoint sends raw requests to the indexer queue.

*By @mavam and @codex.*

### High-level filesystem and object store writers

Four new high-level writer operators serialize events to local filesystems and cloud object stores with rotation, hive-style partitioning, and per-partition unique filenames:

- `to_file` writes to a local filesystem.
- `to_s3` writes to Amazon S3.
- `to_azure_blob_storage` writes to Azure Blob Storage.
- `to_google_cloud_storage` writes to Google Cloud Storage.

Each takes a printing subpipeline, a URL with optional `**` and `{uuid}` placeholders, and rotation parameters. The `**` placeholder expands into a hive partitioning hierarchy based on `partition_by`, and `{uuid}` ensures each partition gets unique destination names:

```tql
subscribe "events"
to_s3 "s3://my-bucket/year=**/month=**/{uuid}.json",
  partition_by=[year, month] {
  write_ndjson
}
```

Files rotate automatically when the configured `max_size` or `timeout` is reached, so long-running pipelines do not produce single huge objects.

*By @raxyte in #6053.*

### Internal memory size function

The new `internal_memory_size` function estimates the size of each event in bytes:

```tql
size = internal_memory_size(this)
```

This is useful for building pipelines that inspect or route events based on their approximate in-memory payload size.

*By @IyeOnline and @codex.*

### Keyed routing and source mode for `parallel`

The `parallel` operator gains two enhancements:

The `jobs` argument is now optional and defaults to the number of available CPU cores:

```tql
subscribe "events"
parallel {
  parsed = data.parse_json()
}
```

The new `route_by` argument routes events to workers deterministically by key. Events with the same `route_by` value always go to the same worker, which is required for stateful subpipelines like `deduplicate` or `summarize`:

```tql
subscribe "events"
parallel route_by=src_ip {
  deduplicate src_ip, dst_ip, dst_port
}
```

Additionally, `parallel` may now be used as a source operator (without upstream input). This spawns multiple independent instances of the subpipeline, which is useful for running the same source pipeline with concurrent connections.

*By @jachris in #5821.*

### Keyed subpipeline routing with `group`

The new `group` operator routes events with the same key through a shared subpipeline. Inside the subpipeline, `$group` refers to the key for that subpipeline:

```tql
group tenant {
  summarize count()
}
```

The subpipeline either emits events—which are forwarded as the operator's output—or ends with a sink, in which case `group` itself becomes a sink. Use `group` when you need keyed routing through a stateful subpipeline, such as a per-tenant sink or a per-session transformation. For grouped aggregations, keep using `summarize`.

*By @jachris in #5980.*

### Live packet capture with `from_nic`

The new `from_nic` operator captures packets from a network interface and emits them as events directly:

```tql
from_nic "eth0"
```

Without an explicit subpipeline, `from_nic` parses the captured PCAP byte stream with `read_pcap`. Provide a subpipeline when you want to change how the byte stream is parsed. Use the `filter` option to apply a Berkeley Packet Filter (BPF) expression so libpcap drops unwanted traffic before parsing:

```tql
from_nic "eth0", filter="tcp port 443"
```

The companion `read_pcap` and `write_pcap` operators have been refreshed: `read_pcap` now also emits a `pcap.file_header` event when `emit_file_headers=true`, which `write_pcap` consumes to preserve the original timestamp precision and byte order. The `pcap.packet` schema's `time.timestamp` field is now a top-level `timestamp` field, and `data` is now a `blob`.

*By @mavam in #6022.*

### Memory-mapped reads in `from_file`

The `from_file` operator now accepts an `mmap=bool` option that uses memory-mapped I/O for reading local files instead of regular reads. This can improve performance for large files:

```tql
from_file "/var/log/large.json", mmap=true {
  read_json
}
```

Defaults to `false`.

*By @raxyte in #6036.*

### Microsoft Graph source operator

Tenzir now includes a Microsoft Graph source operator for reads from Microsoft Graph `v1.0` and `beta` collections with app-only Microsoft Entra authentication and OData pagination.

For example, you can read Entra ID sign-in logs with client credentials and push down OData query options:

```tql
from_microsoft_graph "auditLogs/signIns",
  auth={
    tenant_id: "contoso.onmicrosoft.com",
    client_id: "00000000-0000-0000-0000-000000000000",
    client_secret: secret("ms-graph-client-secret"),
  },
  odata={
    filter: "createdDateTime ge 2026-04-24T00:00:00Z",
    select: ["id", "createdDateTime", "userPrincipalName", "status"],
    top: 1000,
  }
```

The operator emits each object from the response `value` array as a separate event and follows `@odata.nextLink` until the collection is exhausted.

The operator can also use Microsoft Graph delta queries with `delta=true`, storing the returned `@odata.deltaLink` in memory and polling it with a configurable `poll_interval`. OData query options apply to the initial delta request only, subject to Microsoft Graph's resource-specific support, and subsequent polls use the opaque delta link exactly as Microsoft Graph returned it.

It also retries throttled and transient Microsoft Graph requests, respecting `Retry-After` when present.

*By @mavam and @codex in #6165, #6179, and #6182.*

### MySQL source operator

The `from_mysql` operator lets you read data directly from MySQL databases.

Read a table:

```tql
from_mysql table="users", host="localhost", port=3306, user="admin", password="secret", database="mydb"
```

List tables:

```tql
from_mysql show="tables", host="localhost", port=3306, user="admin", password="secret", database="mydb"
```

Show columns:

```tql
from_mysql table="users", show="columns", host="localhost", port=3306, user="admin", password="secret", database="mydb"
```

And ultimately execute a custom SQL query:

```tql
from_mysql sql="SELECT id, name FROM users WHERE active = 1",
           host="localhost",
           port=3306,
           user="admin",
           password="secret",
           database="mydb"
```

The operator supports TLS/SSL connections for secure communication with MySQL servers. Use `tls=true` for default TLS settings, or pass a record for fine-grained control:

```tql
from_mysql table="users", host="db.example.com", database="prod", tls={
  cacert: "/path/to/ca.pem",
  certfile: "/path/to/client-cert.pem",
  keyfile: "/path/to/client-key.pem",
}
```

The operator supports MySQL's `caching_sha2_password` authentication method and automatically maps MySQL data types to Tenzir types.

Use `live=true` to continuously stream new rows from a table. The operator tracks progress using a watermark on an integer column, polling for rows above the last-seen value:

```tql
from_mysql table="events", live=true, host="localhost", database="mydb"
```

By default, the tracking column is auto-detected from the table's auto-increment primary key. To specify one explicitly:

```tql
from_mysql table="events", live=true, tracking_column="event_id",
           host="localhost", database="mydb"
```

*By @mavam and @claude in #5721 and #5738.*

### NATS JetStream operators

Tenzir can now consume from and publish to NATS JetStream subjects with `from_nats` and `to_nats`.

Use `from_nats` to receive one event per message. The raw payload appears in the `message` blob field, and `metadata_field` attaches NATS metadata:

```tql
from_nats "alerts", metadata_field=nats
parsed = string(message).parse_json()
```

Use `to_nats` to publish one message per event. By default, the operator serializes the whole event with `this.print_ndjson()`:

```tql
from {severity: "high", alert_type: "suspicious-login"}
to_nats "alerts"
```

Both operators support configurable connection settings, authentication, and the standard Tenzir `tls` record.

*By @mavam and @codex.*

### OData pagination for from_http

The `from_http` operator now supports `paginate="odata"` for [OData](https://www.oasis-open.org/standard/odata-v4-01-os/) collection responses such as Microsoft Graph:

```tql
from_http "https://graph.microsoft.com/v1.0/users",
  headers={"ConsistencyLevel": "eventual"},
  paginate="odata" {
  read_json
}
```

This mode emits the objects from the response body's top-level `value` array and follows top-level `@odata.nextLink` URLs until no next link is present. The next link can be absolute or relative to the current response URL.

*By @mavam and @codex.*

### Parse TQL records with `read_tql`

The new `read_tql` operator parses an incoming byte stream of TQL-formatted records into events. Each top-level record expression becomes one event:

```tql
load_file "events.tql"
read_tql
```

The input format matches the output of `write_tql`, so `read_tql` is useful for round-tripping data through TQL notation, reading TQL-formatted files, or processing data piped from other Tenzir pipelines.

*By @mavam in #5707.*

### Per-event subpipelines with `each`

The new `each` operator runs a fresh subpipeline for every input event. The event is bound to `$this` inside the subpipeline so it can parametrize the nested logic on a per-event basis:

```tql
from [
  {file: "a.json"},
  {file: "b.json"},
]
each {
  from $this.file
}
```

The subpipeline takes no input from `each`. It either emits events—which are forwarded as the operator's output—and may also end with a sink, in which case `each` itself becomes a sink.

Use `each` for per-event jobs such as a lookup, an export, or a sink whose source depends on the incoming event. For keyed streams that should keep one subpipeline alive per key, use `group` instead.

*By @jachris in #5981.*

### Prometheus shape for `metrics`

The `metrics` operator now accepts `shape="prometheus"` to emit metrics from metrics plugins as canonical `{metric, value, timestamp, labels, type, unit}` records. The default remains `shape="raw"`, which preserves the existing `tenzir.metrics.*` schemas.

*By @mavam and @codex in #6190.*

### Raw byte output with write_all

The new `write_all` operator concatenates one selected `string` or `blob` field into raw bytes:

```tql
from_file "/tmp/report.pdf" {
  read_all binary=true
}
to_file "/tmp/report-copy.pdf" {
  write_all data
}
```

Use it to copy binary payloads, reconstruct byte streams after event processing, or write string fields without separators or escaping.

*By @mavam and @codex.*

### Read from standard input with `from_stdin`

The new `from_stdin` operator reads bytes from standard input through a parsing subpipeline:

```tql
from_stdin {
  read_json
}
```

This is useful when piping data into the `tenzir` executable as part of a shell script or command chain.

*By @raxyte in #5731.*

### Repeat string function

The new `repeat` function repeats a string a given number of times:

```tql
message = "na".repeat(8)
```

```tql
{
  message: "nananananananana",
}
```

*By @mavam and @codex in #6181.*

### Request records for `from_http` pagination

The `from_http` operator now supports returning request records from `paginate` lambdas. This lets APIs keep pagination state in the next request body or headers instead of only in the next URL:

```tql
from_http "https://opensearch.example.com/logs/_search",
  method="post",
  body={size: 500, query: {match_all: {}}},
  paginate=(x => {
    body: {
      size: 500,
      query: {match_all: {}},
      search_after: x.hits.hits[-1].sort,
    },
  } if x.hits.hits != []) {
  read_json
}
```

Returned request records can patch `url`, `method`, `headers`, and `body`. Missing fields inherit from the current request, and `body: null` clears the body.

*By @mavam and @codex.*

### Send events to webhooks with `to_http`

The new `to_http` operator sends each input event as an HTTP request to a webhook or API endpoint. By default, it JSON-encodes the entire event as the request body and sends it as a `POST`:

```tql
subscribe "alerts"
to_http "https://example.com/webhook"
```

`to_http` shares its options with `from_http` and `http`: configure `method`, `body`, `encode`, `headers`, TLS, retries, and pagination per request. Use `parallel` to issue multiple concurrent requests when the target endpoint can keep up with a single pipeline.

This is useful for pushing alerts to webhooks, forwarding events to SIEMs, and calling external APIs once per event.

*By @aljazerzen in #6019.*

### SQS receive controls

The `from_sqs` operator now gives you explicit control over how messages are received from SQS. Use `keep_messages=true` to inspect or replay messages without removing them from the queue, `batch_size=<1..10>` to control how many messages each receive request may return, and `visibility_timeout=<duration>` to override the queue visibility timeout for received messages:

```tql
from_sqs "events", keep_messages=true, batch_size=10, visibility_timeout=30s
```

By default, `from_sqs` keeps deleting each received message after emitting it. With `keep_messages=true`, SQS makes the message visible again after the queue's visibility timeout.

*By @mavam and @codex in #6167 and #6174.*

### Stream pipeline output with `serve_http`

The new `serve_http` operator starts an HTTP server and broadcasts the bytes produced by a nested pipeline to all connected clients:

```tql
from_file "example.yaml"
serve_http "0.0.0.0:8080" {
  write_ndjson
}
```

Clients connect with a `GET` request and receive a continuous HTTP response body. Pick the wire format with the nested pipeline: `write_ndjson` for NDJSON streams, `write_lines` for plain text, and so on. The operator does not buffer output for clients that connect later—each client receives the bytes produced after it connects. TLS, connection limits, and graceful disconnect are all configurable.

*By @aljazerzen in #6070.*

### Synthetic event generation with `anonymize`

The new `anonymize` operator generates synthetic events that share the schemas of its input. The operator first samples a configurable number of input events to learn what schemas are present and to summarize their values, and then replaces the input with generated events that match those schemas:

```tql
subscribe "events"
anonymize count=1000
```

By default, generated values follow the aggregate statistics of the sampled input: null rates, list lengths, numeric ranges, time and duration ranges, boolean and enum frequencies, string and blob lengths and byte frequencies, IP address family frequencies, and subnet prefix length frequencies. Use `fully_random=true` to ignore those statistics and instead pick values uniformly from each type's full range. The optional `seed` argument makes output reproducible.

Use `anonymize` to share representative event traces without leaking the underlying values.

*By @IyeOnline.*

### TQL match statements

TQL now supports statement-level `match` blocks for branching on patterns:

```tql
match action {
  "accept" | "allow" => { verdict = "allowed" }
  "deny" | "drop" => { verdict = "blocked" }
  _ => { verdict = "unknown" }
}
```

Patterns can be constants, exclusive ranges, alternatives separated by `|`, or the final wildcard `_`. Every `match` must include an unguarded final wildcard arm, so Tenzir can prove at compile time that all possible values are covered. This provides a concise alternative to long `else if` chains when routing events by field value.

*By @mavam and @codex.*

### Uncompressed Feather output

The `write_feather` operator now supports `compression_type="uncompressed"` to disable compression entirely. Previously, only `zstd` and `lz4` were accepted:

```tql
to_file "events.feather" {
  write_feather compression_type="uncompressed"
}
```

*By @mavam in #6045.*

## 🔧 Changes

### Add `accept_http` operator for receiving HTTP requests

We added a new operator to accept data from incoming HTTP connections.

The `server` option of the `from_http` operator is now deprecated. Going forward, it should only be used for client-mode HTTP operations, and the new `accept_http` operator should be used for server-mode operations.

*By @lava.*

### Per-schema buffering and default timeout for `batch`

The `batch` operator now maintains separate buffers for each distinct schema. Each buffer has independent timeout tracking and fills until reaching the `limit`, at which point it flushes immediately. Previously, mixed-schema streams could stall waiting for a single combined buffer to fill.

The `timeout` argument now defaults to `1min` instead of an infinite duration, so buffered events are flushed at least once per minute when no new events arrive.

*By @aljazerzen in #5878 and #5906.*

### Preserve categorical order in `chart_bar`

The `chart_bar` and `chart_pie` operators now preserve the incoming row order for categorical x-axis values such as strings, IP addresses, and subnets. This allows users to control bar order with regular TQL operators such as `sort` before charting.

*By @mavam.*

### Region derivation and endpoint logging for SQS

The `from_sqs` and `to_sqs` operators now derive the AWS region from a queue URL when `aws_region` is not set, so passing a full URL such as `https://sqs.us-west-2.amazonaws.com/123456789012/my-queue` works without having to specify the region again:

```tql
from_sqs "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue"
```

Previously, this would fall back to the SDK default region and fail with a SigV4 signature mismatch. Explicit `aws_region`, resolved IAM credentials, and the SDK default still apply in that order when the URL has no region (for example VPC endpoints, LocalStack, or an `AWS_ENDPOINT_URL` override).

SQS API errors and HTTP failures now also include the endpoint URL in their log lines and diagnostic notes, which makes it easier to tell which queue produced an error when multiple SQS pipelines run side by side.

*By @lava in #6168.*

## 🐞 Bug fixes

### Compaction resolves package UDOs at startup

The `compaction` plugin no longer fails to start with `module <package> not found` when a rule's `pipeline` references an operator defined by an installed package. Previously, depending on the order in which the node's components were initialized, the compactor's eager rule-pipeline parse could run before the package manager had published its operator modules to the global registry.

*By @raxyte in #6210.*

### Faster drop_null_fields on heterogeneous data

The `drop_null_fields` operator is now much faster on heterogeneous input with many changing null patterns.

*By @jachris, @mavam, and @codex in #5963.*

### Reduce disk I/O of time-based compaction

Time-based compaction rules no longer cause the node to reprocess data that has already been compacted in a previous run.

*By @lava.*

### SentinelOne Data Lake sink support in the new executor

The `to_sentinelone_data_lake` operator now works in pipelines that run on the new executor. Previously, using it there failed before the pipeline could send events.

```tql
from {message: "hello"}
to_sentinelone_data_lake "https://example.com", token="TOKEN"
```

*By @mavam and @codex in #6081.*

### Top-level package metadata

Packages can now include a top-level `metadata` field for data consumed by external tools. Unknown package keys still fail validation, and the error now points users to `metadata` for non-engine package data.

*By @tobim and @codex in #6149.*
