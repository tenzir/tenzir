Tenzir v6.0.0-rc.2 continues the rollout of the rewritten execution engine that unlocks faster, more capable, and more scalable pipelines. This release candidate includes breaking changes; use the migration guide at https://docs.tenzir.com/guides/tenzir-v6-migration when testing your workloads.

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

### Read from standard input with `from_stdin`

The new `from_stdin` operator reads bytes from standard input through a parsing subpipeline:

```tql
from_stdin {
  read_json
}
```

This is useful when piping data into the `tenzir` executable as part of a shell script or command chain.

*By @raxyte in #5731.*

### Send events to webhooks with `to_http`

The new `to_http` operator sends each input event as an HTTP request to a webhook or API endpoint. By default, it JSON-encodes the entire event as the request body and sends it as a `POST`:

```tql
subscribe "alerts"
to_http "https://example.com/webhook"
```

`to_http` shares its options with `from_http` and `http`: configure `method`, `body`, `encode`, `headers`, TLS, retries, and pagination per request. Use `parallel` to issue multiple concurrent requests when the target endpoint can keep up with a single pipeline.

This is useful for pushing alerts to webhooks, forwarding events to SIEMs, and calling external APIs once per event.

*By @aljazerzen in #6019.*

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

## 🐞 Bug fixes

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
