Tenzir now unifies live and retrospective context matching with the new `context::lookup` operator, and it adds pipeline names to diagnostics and metrics for easier operational correlation. This release also improves export reliability under load and fixes Azure transport errors, HTTP Host headers for non-standard ports, and rebuilt-partition export correctness.

## 🚀 Features

### Include pipeline names in diagnostics and metrics

The `metrics` and `diagnostics` operators now include a `pipeline_name` field.

Previously, output from these operators only identified the source pipeline by its ID. Now the human-readable name is available too, making it straightforward to filter or group results by pipeline name without needing to look up IDs separately.

Please keep in mind that pipeline names are not unique.

*By @IyeOnline and @claude in #5959.*

### Unified context lookups with `context::lookup` operator

The `context::lookup` operator enables unified matching of events against contexts by combining live and retrospective filtering in a single operation.

The operator automatically translates context updates into historical queries while simultaneously filtering all newly ingested data against any context updates.

This provides:

- **Live matching**: Filter incoming events through a context with `live=true`
- **Retrospective matching**: Apply context updates to historical data with `retro=true`
- **Unified operation**: Use both together (default) to match all events—new and historical

Example usage:

```tql
context::lookup "feodo", field=src_ip
where @name == "suricata.flow"
```

*By @IyeOnline in #5964.*

## 🐞 Bug fixes

### Fix crash on Azure SSL/transport errors during read and write operations

Bumped Apache Arrow from 23.0.0 to 23.0.1, which includes an upstream fix for unhandled `Azure::Core::Http::TransportException` in Arrow's `AzureFileSystem` methods. Previously, transport-level errors (e.g., SSL certificate failures) could crash the node during file listing, reading, or writing. Additionally, the direct Azure SDK calls in the blob deletion code paths now catch `Azure::Core::RequestFailedException` (the common base of both `StorageException` and `TransportException`) instead of listing specific exception types.

*By @claude.*

### Fix HTTP Host header missing port for non-standard ports

The `from_http` and `http` operators now include the port in the `Host` header when the URL uses a non-standard port. Previously, the port was omitted, which caused requests to fail with HTTP 403 when the server validates the `Host` header against the full authority, such as for pre-signed URL signature verification.

### Reliable export for null rows in rebuilt partitions

The `export` operator no longer emits partially populated events from rebuilt partitions when a row is null at the record level. Previously, some events could appear with most fields set to `null` while a few values, such as `event_type` or interface fields, were still present.

This makes exports from rebuilt data more reliable when investigating sparse or malformed-looking events.

*By @tobim and @codex in #5988.*

### Reliable recent exports during partition flushes

The `export` command no longer fails or misses recent events when a node is flushing active partitions to disk under heavy load. Recent exports now keep the in-memory partitions they depend on alive until the snapshot completes, which preserves correctness for concurrent import and export workloads.

*By @tobim and @codex.*
