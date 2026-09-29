The `to_clickhouse` operator now keeps fields that have no matching column in a native JSON catch-all column, so events fit into existing tables without losing data. The new `print_avro` function encodes events as Avro binary for message sinks such as Kafka. This release also makes Parquet reads faster and bounded in memory, and makes pipeline shutdown more reliable.

## 🚀 Features

### Avro binary serialization

Use the new `print_avro` function to encode TQL values as Avro binary blobs with an explicit writer schema for message sinks such as Kafka:

```tql
from {id: 42, name: "alice"}
message = print_avro(this, schema={
  type: "record",
  name: "event",
  fields: [
    {name: "id", type: "long"},
    {name: "name", type: "string"},
  ],
})
to_kafka "events", message=message
```

The required `schema` argument accepts a constant TQL record or an Avro JSON schema string. Record fields are encoded in schema order, and missing or extra fields are rejected. Unions select the first matching branch; include a `"null"` branch to encode null values. Values that do not match the schema produce null and a warning.

The output contains no container header, schema, or schema registry framing, so consumers need the matching writer schema. Logical type annotations do not convert units: times and durations encode as nanoseconds unless you convert the input first.

*By @aljaz.*

### ClickHouse catch-all columns

The `to_clickhouse` operator can map events to an existing table and preserve unmapped fields in a native JSON column. Opt in with a column comment:

```sql
CREATE TABLE events (
  `src.port` UInt32,
  extra JSON COMMENT 'tenzir:catch_all'
) ENGINE = MergeTree ORDER BY tuple();
```

The operator maps nested `src.port` values to the dotted column name and packs fields without matching writable columns into `extra`. A value that fails a mapped column's transformation produces a warning and drops the event; it does not move into `extra`. Missing fields use ClickHouse defaults. Tables with the marker refresh their column mappings while the pipeline runs.

For both ordinary JSON columns and the catch-all, the existing JSON printer omits null object fields and null list elements recursively, retaining empty records and lists. Native ClickHouse JSON determines their stored representation. Serialized JSON strings retain the existing writer behavior and malformed objects can fail insertion at the server. The catch-all does not guarantee an exact structured round trip for every Tenzir value.

Marked inserts enable `json_type_escape_dots_in_keys=1`, requiring ClickHouse 25.8 or later. Use the same setting in read queries to restore literal dotted keys. ClickHouse interprets the literal key sequence `%2E` as a dot on read.

Appending to existing tables also supports `UInt16`, `UInt32`, `Int8`, `Int16`, `Int32`, and `Float32` columns without a catch-all marker. These types check range and precision; incompatible values produce a warning and drop the affected event. Unmarked `UInt8` columns retain their existing boolean mapping.

*By @zedoraps.*

### Default packet lengths in write_pcap

The `write_pcap` operator now fills in missing packet lengths, so you only need a link type, a timestamp, and the packet data to write a capture:

```tql
from {
  linktype: uint(1),
  timestamp: 2020-01-02T03:04:05Z,
  data: b"\x00\x01\x02",
}
@name = "pcap.packet"
write_pcap
```

A missing `captured_packet_length` defaults to the size of `data`, and a missing `original_packet_length` defaults to the captured length, which marks the packet as not truncated. Provide `original_packet_length` alone to write a truncated packet. A packet without `linktype` or `data` is an error.

*By @mavam.*

### HTTP URLs and local archives as package sources

The `package_add` operator now accepts HTTP and HTTPS URLs pointing to a self-contained YAML package definition or a `.tar` / `.tar.gz` package archive. Local `.tar`, `.tar.gz`, and `.tgz` files are supported as well:

```tql
package_add "https://example.com/package.yaml"
package_add "https://gitlab.com/api/v4/projects/group%2Frepo/repository/archive.tar.gz?sha=main&path=packages/example"
package_add "https://example.com/packages.tar.gz", path="packages/example"
package_add "packages.tar.gz", path="packages/example"
```

URLs can include query strings and follow HTTP redirects. Use the existing `inputs` argument to configure the downloaded package.

Without `path`, archives must contain exactly one `package.yaml`, which may be at the archive root or nested beneath repository wrapper directories. For archives containing multiple packages, the operator's `path` option selects the directory containing the desired `package.yaml`. Paths are relative to the archive root after removing a single enclosing directory, if present; use `.` to select that root. Paths must be relative and cannot contain `..` components.

Split packages can contain `config.yaml`, `constants.tql`, `pipelines/`, `operators/`, and `examples/`, just like local packages. GitLab's URL-level `path` parameter still filters the download on the server, independently of the operator option, and `sha` selects a branch, tag, or commit. No directory listing is required.

Downloads are limited to 16 MiB, and unpacked archives to 64 MiB and 4096 entries. Archives containing links or unsafe paths are rejected. Local archives have the same 16 MiB input limit and use the same extraction and selection rules. Package-loader warnings remain visible for local directories and both local and remote archives.

*By @lava.*

## 🔧 Changes

### Faster and leaner Parquet reads

`read_parquet` no longer buffers the entire file when it comes first in `from_file`, `from_s3`, `from_google_cloud_storage`, or `from_azure_blob_storage`. It reads the footer, then fetches only the columns and row groups the pipeline needs, and stops once it has enough events. Memory use stays bounded by a few row groups, so you can read files larger than the available memory.

Reads also do less work: `read_parquet` decodes only the nested fields the pipeline uses, skips row groups whose statistics show they cannot match a subsequent `where`, and avoids materializing columns that hold a single value or only nulls.

*By @mavam.*

### More reliable storage maintenance

Storage maintenance now handles changes during rebuilds and compaction more reliably. Disk-budget eviction uses the age of the data rather than the age of its files, and no longer removes data needed by ongoing work. Automatic rebuilds now group data by day and run hourly.

*By @tobim.*

### Pushdown through more operators

Pipeline optimization now carries field selection through `tail`, `reverse`, `slice`, `repeat`, and `deduplicate`, so readers such as `read_parquet` materialize only the fields that downstream operators use. `slice` and `repeat` also pass row limits upstream, so a pipeline like `repeat 3 | head 10` lets the reader stop early, and `repeat 0` and `tail 0` no longer read any input.

*By @mavam.*

### Quieter catalog lookup logging

Catalog lookup result details are now logged at debug level instead of info level, reducing routine informational log noise.

*By @tobim.*

### Smaller BITZ files in Nova pipelines

Nova-enabled `write_bitz` now produces smaller BITZ files for repeated or sparse bitmaps, small integer values, losslessly representable floats, and mixed-type values. `read_bitz` continues to read older BITZ encodings.

For example, run `from {value: 42} | write_bitz` with `--nova=true` to write a BITZ stream with the new encodings.

*By @aljazerzen.*

## 🐞 Bug fixes

### Crash in accept_relp during graceful shutdown

The `accept_relp` operator no longer occasionally crashes when a pipeline shuts down gracefully, for example when stopping a node or pipeline while RELP clients are connected. Previously, stopping the listener could terminate the process with a segmentation fault instead of draining the already accepted messages.

*By @mavam.*

### Reliable stopping of subscriber pipelines

Stopping a pipeline that uses `subscribe` now drains events it has already accepted and finishes without waiting for publishers to stop. Previously, stopping an individual subscriber pipeline could hang indefinitely. Node shutdown still waits for publishers to flush their remaining events.

*By @aljazerzen.*
