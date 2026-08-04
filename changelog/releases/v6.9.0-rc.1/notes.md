The new `to_iceberg` operator writes events directly into Apache Iceberg tables through a REST catalog, creating and evolving the table schema as heterogeneous streams arrive. This release also speeds up node startup for very large catalogs while lowering memory use, and substantially reduces the processor and memory cost of `ocsf::cast`.

## 🚀 Features

### Faster, lower-memory catalog loading at startup

Nodes with a very large number of partitions now start up faster and with a smaller memory footprint.

Partition synopses are now loaded from disk concurrently. The number of worker threads defaults to the hardware concurrency and can be tuned with `tenzir.index.load-concurrency`. This is especially effective on networked storage (such as NFS), where loading is dominated by I/O latency that overlaps across requests.

The new `tenzir.index.lazy-sketches` option defers loading Bloom-filter sketches at startup and loads them on demand when a query needs them. Only string and IP fields use Bloom filters; deferring them drastically lowers resident memory and startup cost for nodes with very many partitions. When a predicate would benefit from a deferred sketch, the catalog loads and checks the surviving candidate partitions one at a time, so equality pruning on these high-cardinality fields is preserved no matter how many partitions match — only a single sketch is resident at a time. Loaded sketches are kept in a memory-bounded LRU cache (`tenzir.index.sketch-cache-bytes`, default 1 GiB) to speed up later queries; the budget caps that warm set, not how much can be pruned. Loading reads the partition's local `.mdx`; on remote stores the partition is conservatively treated as a candidate instead (never a false negative). Numeric and duration min/max synopses and time synopses are never deferred, so range pruning—for example on a timestamp field—is unaffected.

The new `tenzir.index.skip-synopsis-verification` option skips the recursive FlatBuffers verification of partition synopses when reading them at startup, verifying them once when they are written instead. Verification walks the entire buffer and faults in all of its pages—including sketch payloads that are never decoded—so skipping it is the dominant startup saving on networked storage. It trades robustness against on-disk corruption for speed and should only be enabled when the storage backend is trusted.

Loading also avoids a `realpath` and several `stat` calls per partition by resolving the base directories once and reusing file sizes gathered during the initial directory scan, and it now reports progress periodically for large catalogs.

*By @jachris and @claude in #6368.*

### Native output to Apache Iceberg tables

The new `to_iceberg` operator writes events to Apache Iceberg tables through a REST catalog:

```tql
subscribe "ocsf"
ocsf::cast
to_iceberg "security.ocsf",
  catalog="https://catalog.example.com",
  partition_by=[class_uid, day(time)]
```

The operator creates missing tables from the first arriving events (`mode` selects between `create_append`, `create`, and `append`) and evolves the table schema continuously. It adds new fields at any nesting depth through a metadata-only schema update before writing data files that carry them, so heterogeneous streams like OCSF converge into one wide table without name mappings or manual `ALTER TABLE` steps. Existing columns widen in place where the Iceberg spec allows it (`int` to `long`, `float` to `double`). Values that still do not fit are written as null with a warning; for required columns the operator fails with an error instead, so data is never lost silently.

`partition_by` uses Iceberg's hidden partitioning: it accepts field paths and the transforms `year`, `month`, `day`, `hour`, `bucket`, and `truncate`, without materializing helper columns. Data files are zstd-compressed Parquet with field IDs and per-column metrics, rotated by `max_size` and `timeout` and committed as Iceberg snapshots that retry on top of concurrent updates. Partitions buffer under a shared `buffer_size` budget, so high partition cardinality produces fewer, larger files instead of many small ones.

The operator connects to S3 and S3-compatible object stores (`aws_iam`, `s3_endpoint`, `s3_path_style`), catalogs taking bearer tokens (`token`), AWS Glue and Amazon S3 Tables (`catalog_aws_service`), and Google BigLake with `gs://` storage (`gcp_service_account_key`, or `gcp_auth=true` for Application Default Credentials).

*By @zedoraps.*

### Optionally validate batches read from persisted stores

The new `tenzir.validate-store-batches` option fully validates every Arrow record batch read from a persisted Feather store. This helps diagnose on-disk corruption during exports and rebuilds, including invalid values such as malformed UTF-8 that structural validation does not detect. The option is disabled by default because full validation scans all values in every batch.

*By @raxyte.*

## 🔧 Changes

### Faster variant encoding in ocsf::cast

`ocsf::cast` with `encode_variants=true` now encodes free-form objects such as `unmapped` significantly faster, which substantially lowers CPU usage for pipelines that cast high-volume streams with populated `unmapped` fields.

*By @tobim.*

### Lower resource usage for null-filling in ocsf::cast

`ocsf::cast` with `null_fill=true` now uses significantly less memory and CPU. Previously, the operator rebuilt the null-filled columns for every batch, which roughly doubled peak memory usage for schema-complete casting of high-volume streams.

*By @tobim.*

## 🐞 Bug fixes

### AWS profile selection via environment variables

Operators that authenticate with AWS now honor the `AWS_PROFILE` environment variable, falling back to the legacy `AWS_DEFAULT_PROFILE`. Previously, both variables were ignored and credentials always resolved from the `default` profile.

Profiles configured with `credential_process` or an SSO session now work as well:

```sh
AWS_PROFILE=analytics tenzir 'from {x: 1} | to_s3 "s3://bucket/x.json"'
```

If your node runs with `AWS_PROFILE` set unintentionally, it now authenticates with that profile instead of `default`—unset the variable to keep the old behavior.

*By @zedoraps.*

### Consistent value formatting in direct JSON sink output

Sink operators that serialize events to JSON internally, such as `to_kafka` and `to_amqp`, now format `time`, `duration`, `ip`, and `subnet` values identically to `write_json` and `write_ndjson`. Previously, timestamps rendered as `2026-07-30 12:00:00.000000000` instead of the ISO 8601 form `2026-07-30T12:00:00Z`.

*By @tobim.*

### Reject compile-invalid definitions on pipeline update

Updating a pipeline to a definition that parses but does not compile (for example, one referencing an unknown operator, producing byte output, or not ending in a sink) no longer silently replaces the previous working definition. The update is now rejected with the compile diagnostics, matching the validation that already happens in the Explorer.

*By @gitryder and @claude.*
