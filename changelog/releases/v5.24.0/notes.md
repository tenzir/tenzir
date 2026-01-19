This release adds XML parsing functions (`parse_xml` and `parse_winlog`) for analyzing XML-formatted logs including Windows Event Logs. It also introduces the `parallel` operator for parallel pipeline execution, fixes a socket leak in `from_http` that could cause resource exhaustion, and includes several stability fixes for gRPC operators and the pipeline API.

## 🚀 Features

### Easy parallel pipeline execution

The `parallel` operator executes a pipeline across multiple parallel pipeline instances to improve throughput for computationally expensive operations. It automatically distributes input events across the pipeline instances and merges their outputs back into a single stream.

Use the `jobs` parameter to specify how many pipeline instances to spawn. For example, to parse JSON in parallel across 4 pipeline instances:

```tql
from_file "input.ndjson"
read_lines
parallel 4 {
  this = line.parse_json()
}
```

*By @raxyte in #5632.*

### Per-pipeline memory consumption metrics

The new `tenzir.metrics.operator_buffers` metrics track the total bytes and events buffered across all operators of a pipeline. The metrics are emitted every second and include:

- `timestamp`: The point in time when the data was recorded
- `pipeline_id`: The pipeline's unique identifier
- `bytes`: Total bytes currently buffered
- `events`: Total events currently buffered (for events only)

Use `metrics "operator_buffers"` to access these metrics.

*By @jachris in #5644.*

### XML parsing functions for TQL

The new `parse_xml` and `parse_winlog` functions parse XML strings into structured records, enabling analysis of XML-formatted logs and data sources.

The `parse_xml` function offers flexible XML parsing with XPath-based element selection, configurable attribute handling, namespace management, and depth limiting. It supports multiple match results as lists and handles both simple and complex XML structures.

The `parse_winlog` function specializes in parsing Windows Event Log XML format, automatically finding Event elements and transforming EventData/UserData sections into properly structured fields.

Both functions integrate with Tenzir's multi-series builder for schema inference and type handling.

*By @mavam and @claude in #5640 and #5645.*

## 🔧 Changes

### Duplicate diagnostics only suppressed for 4 hours

Repeated warnings and errors now resurface every 4 hours instead of being suppressed forever. Previously, once a diagnostic was shown, it would never appear again even if the underlying issue persisted. This change helps users notice recurring problems that may require attention.

*By @raxyte and @claude in #5652.*

### Event-based rate limiting for throttle operator

The `throttle` operator now rate-limits events instead of bytes. Use the `rate` option to specify the maximum number of events per window, `weight` to assign custom per-event weights, and `drop` to discard excess events instead of waiting. The operator also emits metrics for dropped events.

*By @raxyte in #5642.*

## 🐞 Bug fixes

### Crashes during gRPC operator shutdown

We fixed bugs in several gRPC-based operators:

- A potential crash in `from_velociraptor` on shutdown.
- Potentially not publishing final messages in `to_google_cloud_pubsub` on shutdown.
- A concurrency bug in `from_google_cloud_pubsub` that could cause a crash.

*By @mavam and @claude in #5661.*

### Error propagation in every and cron operators

The `every` and `cron` operators now correctly propagate errors from their subpipelines instead of silently swallowing them.

*By @raxyte in #5632.*

### Fixed `from_kafka` not producing events

We fixed a bug in `from_kafka` that would cause it to not produce events.

*By @IyeOnline in #5659.*

### Missing events when using `in` with `export`

The `export` operator incorrectly skipped partitions when evaluating `in` predicates with uncertain membership. This caused queries like `export | where field in [values...]` to potentially miss matching events.

*By @raxyte in #5660.*

### Socket leak in `from_http`

The `from_http` operator sometimes left sockets in `CLOSE_WAIT` state instead of closing them properly. This could lead to resource exhaustion on long-running nodes receiving many HTTP requests.

*By @jachris and @claude in #5647.*

### Timezone handling in static binary

The `format_time` and `parse_time` functions in the static binary now correctly use the operating system's timezone database.

*By @tobim and @claude in #5649.*

### Unresponsive pipeline API

Previously, it was possible for the node to enter a state where the internal pipeline API was no longer responding, thus rendering the platform unresponsive.

*By @jachris in #5651.*
