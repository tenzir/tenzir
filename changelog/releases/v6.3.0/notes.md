Tenzir now drains in-flight data before stopping pipelines or shutting down a node, so you no longer lose events on shutdown. This release also improves diagnostics from user-defined operators with call-site annotations that trace errors back to the top-level pipeline, and adds a from_clickhouse source operator for reading directly from ClickHouse.

## 🚀 Features

### Automatic input format detection

The new `read_auto` operator detects common input formats before choosing a reader:

```tql
from_stdin {
  read_auto
}
```

Detection dry-runs the actual parsers on a bounded probe of the input. When multiple readers are capable of consuming the bytes, the most specific format wins, e.g., Suricata EVE beats generic NDJSON. Formats that accept arbitrary text, such as space-separated values, are never auto-detected; select their readers explicitly instead.

By default, `read_auto` fails when detection finds no unique match. Use `fallback="lines"` or `fallback="all"` to opt into generic line or whole-input reading for otherwise unknown input.

*By @mavam and @codex in #6191.*

### Call-site annotations in diagnostics from user-defined operators

Diagnostics from inside a user-defined operator now include source context from the UDO as well as a "called from here" trace back to the top level pipeline. This makes it possible to immediately locate the offending call when a diagnostic is emitted deep in a nested operator:

```tql
from {}
test::error
```

```go
error: assertion failure
 --> <packages/test:error>:2:10
  |
2 |   assert false
  |          ^^^^^
  |
 --> <input>:2:1
  |
2 | test::error
  | ^^^^^^^^^^^ called from here
  |
```

Previously, such diagnostics contained no location information, making it difficult to associate them with a specific call in the pipeline.

*By @IyeOnline and @aljazerzen in #6350.*

### Case-insensitive text functions

String functions now accept an `ignore_case` argument for case-insensitive matching, removing the need to call `to_lower()` on both sides of a comparison. It is available on `starts_with`, `ends_with`, `contains`, `replace`, and `split`, and defaults to `false`. Comparison uses full Unicode case folding, so `"Aljaž".starts_with("aljaž", ignore_case=true)` is `true` and `equals("STRASSE", "straße", ignore_case=true)` is `true`.

The new `equals(x, y, ignore_case=false)` function performs string equality with optional case folding, covering the case-insensitive equivalent of the `==` operator.

*By @aljazerzen in #6378.*

### Configure an HTTP proxy for outbound operators

Tenzir nodes can now route outbound traffic from operators through an HTTP/HTTPS proxy. Set `tenzir.http-proxy` for `http://` targets and `tenzir.https-proxy` for `https://` and gRPC targets in `tenzir.yaml` (or via the matching `TENZIR_HTTP_PROXY` / `TENZIR_HTTPS_PROXY` environment variables) to URLs such as `http://proxy.example.com:3128`; Basic-auth userinfo is supported.

`tenzir.no-proxy` accepts the same suffix-list syntax as the `NO_PROXY` environment variable and lets specific destinations bypass the proxy. CIDR entries match IP-literal destinations. `TENZIR_NO_PROXY` provides the matching environment override. Loopback addresses (`localhost`, `127.0.0.0/8`, `::1`) always bypass.

The setting applies to operators that perform outbound HTTP, HTTPS, or gRPC requests — `from_s3` / `to_s3`, `from_google_cloud_storage` / `to_google_cloud_storage`, `from_azure_blob_storage` / `to_abs`, `from_http` / `to_http`, `to_elasticsearch`, `from_sqs` / `to_sqs`, `from_amazon_cloudwatch` / `to_amazon_cloudwatch`, `from_velociraptor`, `from_google_cloud_pubsub` / `to_google_cloud_pubsub`, `to_google_cloud_logging`, the libcurl-backed `from "https://…"` / `to "https://…"` connectors, and the Tenzir Platform WebSocket.

For gRPC-backed operators, gRPC Core accepts only `http://` proxy URLs; it rejects `https://` proxy URLs in its own proxy mapper.

The proxy URL must include an explicit port, since the default-port behaviour of the underlying SDKs is not portable.

Caveat: the GCS default credential chain first probes the metadata server at `metadata.google.internal`, which is on google-cloud-cpp's hard-coded `NO_PROXY` list. Workload-identity / metadata lookups will not traverse the proxy; anonymous or explicitly-credentialed flows do.

The proxy URL lives plaintext in YAML; promotion to a `secret` is deferred.

*By @raxyte in #6222.*

### Graceful pipeline shutdown with data draining

Stopping a pipeline or shutting down the node now drains in-flight data before terminating, instead of discarding it. Source operators receive a graceful stop signal and can finish outstanding work before the pipeline shuts down.

A configurable grace period (`tenzir.shutdown-grace-period`, default 3 minutes) bounds how long the system waits. After the grace period expires, remaining pipelines are force-killed. Setting the grace period to `0s` (or any non-positive value) waits indefinitely, so pipelines are never force-killed and drain fully before the node quits.

*By @aljazerzen in #6215.*

### New `from_clickhouse` source operator

The new `from_clickhouse` operator fetches data from a ClickHouse server. You can either read an entire table:

```tql
from_clickhouse table="events"
```

Or run a custom SQL query directly:

```tql
from_clickhouse sql="SELECT * FROM events WHERE severity >= 3 ORDER BY time DESC"
```

*By @IyeOnline in #6048.*

### Re-added pipeline::detach and pipeline::add

The `pipeline::detach` and `pipeline::add` operators are available again on the new executor.

> **Note:** Prefer the `tenzir-test` `suite` feature with `mode: parallel` for coordinating background pipelines in tests.

*By @IyeOnline in #6364.*

## 🔧 Changes

### ClickHouse Bool columns for Tenzir booleans

The `to_clickhouse` operator now creates ClickHouse `Bool` columns for Tenzir boolean fields instead of `UInt8` columns.

For example, a Tenzir field such as `active: true` is now stored as `Bool` in ClickHouse when creating a table with `to_clickhouse`.

*By @IyeOnline and @codex in #6048.*

### More efficient conditional expressions

Conditional expressions such as `x if y else z` are now significantly more efficient than before when both branches share the same type or one side is `null`. Downstream operators in particular benefit from this change, making certain pipelines much faster as a result.

*By @jachris in #6246.*

## 🐞 Bug fixes

### Correct null handling in bloom-filter context lookups

The `bloom-filter` context no longer matches `null` values when the filter was populated with empty strings. Now null values no longer match the context.

*By @IyeOnline and @claude in #6252.*

### Crash fix for list.add with null-typed record fields

`list.add` no longer crashes when the existing list contains record elements where one or more fields previously held only null values. Previously, calling `list.add` with a new element that provided a real value (for example a hostname string) for such a field would trigger an internal assertion failure. The function now correctly widens null-typed fields to accommodate the new element's type.

*By @IyeOnline and @claude in #6361.*

### Fix duplicate key handling in `parse_kv` and `read_kv`

`parse_kv` and `read_kv` now upgrade a field to a list of values when a key occurs more than once in a single event, as documented. Previously, a repeated key silently kept only its last value.

*By @lava in #6369.*
