This release candidate improves correctness for export workloads by preventing missing or partially populated events while partitions are rebuilt or flushed under load. It also fixes HTTP requests to non-standard ports by including the port in the Host header, restoring compatibility with strict servers such as pre-signed URL endpoints.

## 🐞 Bug fixes

### Fix HTTP Host header missing port for non-standard ports

The `from_http` and `http` operators now include the port in the `Host` header when the URL uses a non-standard port. Previously, the port was omitted, which caused requests to fail with HTTP 403 when the server validates the `Host` header against the full authority, such as for pre-signed URL signature verification.

### Reliable export for null rows in rebuilt partitions

The `export` operator no longer emits partially populated events from rebuilt partitions when a row is null at the record level. Previously, some events could appear with most fields set to `null` while a few values, such as `event_type` or interface fields, were still present.

This makes exports from rebuilt data more reliable when investigating sparse or malformed-looking events.

*By @tobim and @codex in #5988.*

### Reliable recent exports during partition flushes

The `export` command no longer fails or misses recent events when a node is flushing active partitions to disk under heavy load. Recent exports now keep the in-memory partitions they depend on alive until the snapshot completes, which preserves correctness for concurrent import and export workloads.

*By @tobim and @codex.*
