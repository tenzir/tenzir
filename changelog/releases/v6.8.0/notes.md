The new `from_splunk` operator lets nodes run bounded searches against a Splunk Search Head and emit results as events. `to_clickhouse` now re-batches events before insert for higher throughput and supports writing directly into JSON columns. This release also makes nodes resilient to corrupt or truncated partitions, quarantining them instead of crashing repeatedly.

## 🚀 Features

### Improved `to_clickhouse`

The `to_clickhouse` operator now re-batches events internally before inserting them. Instead of sending one INSERT per incoming table slice, it accumulates events per target table and flushes them once they reach `max_batch_rows` (default `65536`) or have waited `batch_timeout` (default `1s`). This coalesces the tiny slices produced by heterogeneous input, such as OCSF, into far larger and more efficient inserts.

`to_clickhouse` can now also write `string` values directly into a ClickHouse `JSON` column, as long as the string is a JSON object. This lets you serialize heterogeneous events to JSON yourself and collapse them into a single schema for maximum insert throughput.

*By @IyeOnline in #6445.*

### Splunk search input operator

The new `from_splunk` operator runs a bounded search against a Splunk Search Head and emits every result as an event:

```tql
from_splunk "https://splunk.example.com:8089",
  search="search index=main sourcetype=linux_secure",
  earliest=now() - 15min,
  latest=now() - 5min,
  headers={Authorization: secret("splunk-authorization")}
```

The `earliest` and `latest` bounds take `time` values like the ones above or strings in Splunk's native relative-time syntax, such as `earliest="-15m"` or `earliest="-1h@h"` for snapping to the full hour.

The operator supports secret-valued authorization headers, TLS configuration, request timeouts, and retries for recurring collection pipelines.

*By @zedoraps and @codex in #6447.*

## 🔧 Changes

### Reduced operator channel buffer size

The buffer capacity between operators is now 32 MiB, reduced from 100 MiB. This bounds how much in-flight data a pipeline holds: a pipeline with 11 operators now buffers at most 352 MiB instead of up to 1.1 GiB. Smaller buffers reduce the time it takes to stop a pipeline gracefully.

*By @aljazerzen and @claude in #6462.*

### Workspace and node id in platform connection logs

The log message emitted when a node connects to the Tenzir Platform now names the workspace it authenticated into. Nodes that self-register with a workspace registration key additionally print their own node id:

```
node connected to platform via wss://ws.tenzir.app:443/production into workspace t-abcd1234 as node ne-1a2b3c4d
```

*By @lava in #6450.*

## 🐞 Bug fixes

### `parse_syslog` rejects partially parsed input

`parse_syslog` now requires the parser to consume the entire input. Previously, a message that parsed only partially was still accepted, and any trailing bytes the parser did not reach were silently dropped. Such input is now rejected with a diagnostic instead of being truncated.

*By @jachris and @claude in #6454.*

### Graceful S3 close failures in legacy pipelines

Pipelines running with `neo: false` no longer abort the node when closing an S3 output stream fails, including S3-backed `to_hive` pipelines. The close failure now stops only the affected pipeline with an error.

*By @jachris and @codex in #6457.*

### Gracefully handle rebuild and compaction failures

Corrupt partitions could previously bring down more than just the rebuild that touched them. The compactor now skips partitions that failed to compact instead of retrying them on every run. A catalog lookup over a damaged partition synopsis no longer terminates the node, and neither does importing events whose record batches fail to concatenate — the affected buffer is dropped with an error.

A corrupt or truncated partition or store file could also crash a node repeatedly and stall reads across the whole node, even though `import` kept working. The catalog now quarantines such a partition as a single operation: it moves the store file into a `quarantined` directory for later inspection, deletes the partition's other on-disk files, and removes it from the catalog, so `rebuild` no longer picks it up again and continues rebuilding all other partitions normally. Each quarantine is logged with the file and error, tracked in the rebuilder's status, and reported through a new `tenzir.metrics.rebuild_quarantine` event, emitted only when a partition is actually quarantined.

*By @IyeOnline and @claude in #6452.*

### read_parquet supports microsecond timestamps

Reading Parquet files written by other systems crashed when they contained timestamp columns with non-nanosecond precision, such as files written by Apache Spark or to Apache Iceberg tables, which mandate microseconds. `read_parquet` now converts such columns to Tenzir's nanosecond precision on read.

*By @zedoraps and @claude in #6431.*

### Stopping a recovered pipeline no longer crashes the node

Fixes a node crash (`assertion 'pipeline.executor' failed`) that could occur when stopping or force-stopping a pipeline that was previously placed in an invalid "half-stopped" state.

*By @aljazerzen in #6456.*

### Validate user-defined operator names in `tenzir.yaml`

Operators defined in `tenzir.yaml` under `tenzir.operators` now have their names validated the same way as operators defined in packages. Previously, an invalid name such as `a-b` was silently accepted at startup even though the resulting operator could never be referenced from a pipeline.

*By @IyeOnline in #6455.*

### Web identity token retrieval through proxies

Nodes that use an outbound proxy now bypass it for IPv4 and IPv6 link-local metadata endpoints. This restores `aws_iam.web_identity.token_endpoint` authentication when the token is served from local instance metadata.

*By @tobim and @codex in #6458.*
