This release introduces the `window` operator for event-time windowing, which groups streaming events into configurable time windows driven by event timestamps rather than wall-clock time. It also adds several new operators — `ai::prompt` for LLM integration, `from_microsoft_sql` for SQL Server reads, and `to_prometheus` for Prometheus Remote Write — along with Crypto-PAn decryption and multiple reliability fixes.

## 🚀 Features

### Add the `ai::prompt` operator

The new `ai::prompt` operator sends each input event to an OpenAI-compatible Responses API endpoint and writes a result record back into the event:

```tql
from {message: "summarize this"}
ai::prompt model="gpt-4.1-mini"
```

By default the operator serializes `this` as compact JSON, writes the generated text plus model, token usage, and latency metadata to `ai.prompt`, and uses the local Ollama endpoint at `http://127.0.0.1:11434/v1`. Override `endpoint` to use another OpenAI-compatible service.

*By @mavam and @codex in #6260.*

### Chunk stream operators

TQL now includes `read_chunks` and `write_chunks` for converting between byte streams and records with a `bytes` field.

For example, you can capture arbitrary output as records and turn it back into a byte stream later:

```tql
from {line: "hello"}, {line: "world"}
write_lines
read_chunks
write_chunks
read_lines
```

This makes it easier to inspect, buffer, transform, and roundtrip chunked byte streams inside a pipeline.

*By @aljazerzen and @codex.*

### Crypto-PAn decryption

The new `decrypt_cryptopan` function decrypts IP addresses that were encrypted with `encrypt_cryptopan` and the same seed. The function can also force the Crypto-PAn domain with `family="ipv4"` or `family="ipv6"` when decrypting an ambiguous ciphertext.

*By @mavam and @codex in #6259.*

### Event-time windowing with the window operator

The new `window` operator groups streaming events into event-time windows and runs a subpipeline for each window:

```tql
window size=10min, every=1min, on=ts {
  summarize failures=count()
  start = $window.start
  end = $window.end
}
```

Unlike `every`, which reruns a subpipeline on a wall-clock schedule, `window` operates on **event time**: it assigns each event to windows by the timestamp that `on` evaluates to, and its internal clock is driven entirely by the timestamps of the incoming events. Windows are aligned to the Unix epoch and are either tumbling (omit `every`) or overlapping/hopping (`every < size`).

`tolerance` sets how much out-of-order lag the clock waits for before a window closes; events that arrive after their window closed are dropped with a warning.

`idle_timeout` makes `window` also well suited to low-volume streams: a window is emitted once it has been inactive for the given duration, so results arrive promptly even when the next event is far off, instead of waiting for it or for the end of the input.

Each window's `$window.start` and `$window.end` are available inside the subpipeline, which may also end in a sink.

*By @jachris in #6253.*

### Microsoft SQL Server source operator

The new `from_microsoft_sql` operator reads rows from Microsoft SQL Server:

```tql
from_microsoft_sql table="dbo.users",
                   host="sql.example.net",
                   user="tenzir",
                   password=secret("mssql-password"),
                   database="telemetry"
```

It supports table reads, custom `sql` or `query` statements, schema inspection with `show="tables"` and `show="columns"`, SQL authentication, secret-backed passwords, TLS, and live polling via integer tracking columns. Result decoding covers SQL Server scalar values including integers, booleans, floats, decimals, money values, strings, binary data, `uniqueidentifier`, date/time values, XML, and JSON stored as text.

*By @tobim and @codex.*

### Prometheus Remote Write sink

The new `to_prometheus` operator sends metric events to Prometheus Remote Write receivers.

For example:

```tql
from {
  metric: "http_requests_total",
  value: 42,
  timestamp: 2026-05-15T10:00:00Z,
  labels: {method: "GET", status: 200},
}
to_prometheus "https://prometheus.example/api/v1/write"
```

The operator supports Prometheus Remote Write v1 by default and can send Remote Write v2 payloads with `protobuf_message="io.prometheus.write.v2.Request"`.

*By @mavam and @codex.*

## 🔧 Changes

### Null list spread warnings

Spreading `null` into a list no longer emits a warning. This makes optional list-building expressions work without an explicit `else []` fallback:

```tql
from {xs: null}
items = [1, ...xs, 2]
```

The expression produces `[1, 2]`, and `concatenate`, `append`, and `prepend` follow the same warning-free behavior for `null` list inputs.

*By @mavam and @codex.*

## 🐞 Bug fixes

### Crash when passing a non-lambda to map or where

Calling the `map` or `where` list functions with a non-lambda argument now fails with a clear `expected a lambda` diagnostic instead of aborting the pipeline with an internal error.

For example, the following pipeline previously crashed and now reports a proper error:

```tql
from {xs: [1, 2, 3]}
xs = xs.map(null)
```

*By @jachris in #6256.*

### Fix `where` substring filters dropping all events from `subscribe`

A `where` filter that checks for a substring with the literal on the left, such as `where "TRAFFIC" in log`, incorrectly removed every event when reading from `subscribe`. Such filters now match correctly again.

*By @jachris.*

### Platform connection during node startup

Tenzir Nodes now connect to the Tenzir Platform during startup before waiting for the local index to become available. Previously, Platform connectivity could be delayed while the index was still initializing.

*By @tobim and @codex in #6268.*

### Rebuild memory limits

Rebuilds now limit how much partition data they load into memory at once. This reduces the risk that automatic rebuilds or `tenzir-ctl rebuild start` cause the node to run out of memory when many partitions are selected.

When memory is scarce, rebuilds load fewer partitions in one batch and retry the remaining partitions later instead of materializing the full selected input set up front.

*By @tobim and @codex in #6216.*

### Static pipeline startup validation

Tenzir now validates all configured and packaged pipelines before starting any of them during node startup. Previously, a deployment with multiple configured pipelines could start some valid pipelines and only then abort after discovering that a later pipeline was invalid.

*By @tobim and @codex in #6248.*
