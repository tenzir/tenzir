Tenzir now scales pipeline execution across the node, including file ingestion and periodic aggregation. This release also adds native OpenTelemetry ingestion and improves reliability and format handling.

## 🚀 Features

### Native OTLP ingestion

Receive OpenTelemetry logs, metrics, and traces directly with the new `accept_otlp` operator over OTLP/HTTP or OTLP/gRPC:

```tql
accept_otlp "0.0.0.0:4318", transport="http"
```

To receive the same signals over OTLP/gRPC, select the gRPC transport:

```tql
accept_otlp "0.0.0.0:4317", transport="grpc"
```

The receiver supports JSON and binary Protobuf over HTTP, unary gRPC, gzip compression, TLS and mTLS, signal filtering, bounded request sizes and concurrency, and lossless list-shaped attributes by default. Set `schema="record"` for direct access to literal attribute keys.

*By @mavam.*

### Node-wide parallelism

Parallelism is now configurable for an entire node with the `tenzir.parallelism` option:

```yaml
tenzir:
  parallelism: max,limit_partitions=8
```

Every pipeline that does not request a degree itself runs with the configured parallelism. Previously, the only ways to enable parallelism were the `--parallelism` option of the `tenzir` binary, which does not apply to pipelines running in a node, and a `// parallelism:` comment in a pipeline's leading comment lines.

The option takes the same values as `--parallelism` and the `// parallelism:` directive, and can also be set through the `TENZIR_PARALLELISM` environment variable. A `// parallelism:` directive in a pipeline takes precedence over `--parallelism`, which in turn takes precedence over `tenzir.parallelism`. Use `// parallelism: disabled` to opt a single pipeline out of the configured degree:

```tql
// parallelism: disabled
from_file "in.json"
where dest_port == 443
```

Invalid values now name their origin, so a typo in the configuration is no longer mistaken for a typo in the pipeline:

```
error: invalid parallelism value in `tenzir.parallelism` configuration option
```

*By @aljazerzen and @claude.*

### Parallelize file-reading operators

`from_file`, `from_s3`, `from_google_cloud_storage`, and `from_azure_blob_storage` now run as multiple instances when the pipeline is parallelized. The instances split the discovered files among themselves, so a directory with many files is read on several cores instead of one:

```tql
from_file "/var/log/**/*.json" {
  read_json
}
```

Which instance reads a file depends only on its path. Every file is therefore read, removed, and renamed exactly once, both when rescanning in `watch` mode and after a restart. Each instance reads its files one after another, so the degree of parallelism determines how many files Tenzir reads at the same time.

*By @aljazerzen.*

## 🔧 Changes

### Faster summarize on interleaved group keys

The `summarize` operator is significantly faster on inputs whose group keys arrive interleaved, such as a stream of connection logs grouped by `src_ip, dest_ip` where consecutive events rarely belong to the same group:

```tql
summarize src_ip, dest_ip, bytes=sum(bytes), events=count()
```

Grouping wide events, or grouping by several fields at once, benefits most. Inputs that already arrive sorted by their group key can be slightly slower, most noticeably when nearly every event forms its own group.

*By @aljazerzen and @claude.*

### Flat names for builtin operators and functions

Builtin operators and functions no longer live in modules. Modules are now reserved exclusively for packages, so every builtin entity that used a module-qualified name now uses a flat name:

| Before                         | After                         |
| ------------------------------ | ----------------------------- |
| `ai::prompt`                   | `ai_prompt`                   |
| `context::create_bloom_filter` | `context_create_bloom_filter` |
| `context::create_geoip`        | `context_create_geoip`        |
| `context::create_lookup_table` | `context_create_lookup_table` |
| `context::enrich`              | `context_enrich`              |
| `context::erase`               | `context_erase`               |
| `context::inspect`             | `context_inspect`             |
| `context::list`                | `context_list`                |
| `context::load`                | `context_load`                |
| `context::lookup`              | `context_lookup`              |
| `context::remove`              | `context_remove`              |
| `context::reset`               | `context_reset`               |
| `context::save`                | `context_save`                |
| `context::update`              | `context_update`              |
| `ocsf::cast`                   | `ocsf_cast`                   |
| `ocsf::derive`                 | `ocsf_derive`                 |
| `ocsf::trim`                   | `ocsf_trim`                   |
| `ocsf::category_name`          | `ocsf_category_name`          |
| `ocsf::category_uid`           | `ocsf_category_uid`           |
| `ocsf::class_name`             | `ocsf_class_name`             |
| `ocsf::class_uid`              | `ocsf_class_uid`              |
| `ocsf::type_name`              | `ocsf_type_name`              |
| `ocsf::type_uid`               | `ocsf_type_uid`               |
| `package::add`                 | `package_add`                 |
| `package::list`                | `package_list`                |
| `package::remove`              | `package_remove`              |
| `pipeline::activity`           | `pipeline_activity`           |
| `pipeline::detach`             | `pipeline_detach`             |
| `pipeline::list`               | `pipeline_list`               |
| `pipeline::run`                | `pipeline_run`                |

Before:

```tql
context::enrich "feodo", key=src_ip
```

After:

```tql
context_enrich "feodo", key=src_ip
```

The old spellings keep working until the next major release: they resolve to the new name and emit a deprecation warning, for example:

```
warning: `context::enrich` is deprecated
  = note: modules are reserved for packages
  = hint: use `context_enrich` instead
```

Package entities always take precedence over this compatibility path, so a package can now use any module name, including `context`, `ocsf`, `package`, `pipeline`, and `ai`, without its operators and functions being shadowed by builtins. Entities that exist only under their module-qualified name, such as the deprecated `ocsf::apply`, keep resolving through the same path.

*By @mavam.*

### Parallel summarize with periodic emission

The `summarize` operator can run on multiple cores, even when `emit: <duration>`. This means that outputs of groups might not be emitted at aligned intervals.

For example:

```tql
// parallelism: 8
from ...
summarize key, count(), options={emit: 1h, mode: "reset"}
```

... given two input events:

```tql
{key: 'a'}  # at 7:00
{key: 'b'}  # at 7:20
```

... may now emit an aggregate for `a` at 8:00, and an aggregate for `b` at 8:20. This depends on how parallel jobs are distributed, and can still result in both aggregates being emitted at 8:00. When using `// parallelism: 1`, both aggregates are guaranteed to emit at 8:00.

When it is important that the intervals are aligned, use `window` operator instead.

*By @aljazerzen.*

### Rework `parallel` operator

A `parallel` block is now semantically transparent. Operators inside it observe the same events, in the same shape, as they would without it, so you no longer have to reason about how many instances of an operator exist or about a boundary between an outer and an inner pipeline.

The `route_by` option has been deprecated and is now automatically inferred.

`parallel N { … }` raises the degree only for parallelizable operators. Operators that cannot run as multiple instances continue to run as one. Sources are among them, so `parallel N { … }` around a source no longer produces `N` copies of every event.

*By @aljazerzen.*

### Sequential pipelines without fused channels

Pipelines that request no parallelism now behave exactly as if they carried a `// parallelism: disabled` directive. This is what `--parallelism` documented all along, but the effective default also fused the channels between parallelizable operators, which made them process one item at a time end-to-end. Such pipelines now use plain buffered channels instead.

Setting `fused=none` explicitly no longer restricts channels to a small buffer either. It now consistently means "plain buffered channels throughout", at the cost of more memory at a high degree of parallelism:

```tql
// parallelism: 6,fused=none
```

To get the previous default behavior for a single pipeline, request degree one explicitly with `// parallelism: 1`, or set `tenzir.parallelism: 1` to get it for a whole node.

*By @aljazerzen and @claude.*

## 🐞 Bug fixes

### Correct parsing of trailing empty CSV values

The CSV parser now recognizes a trailing empty value instead of mistaking it for a missing value and emitting a warning. It parses as `null` when it matches `null_value`, and as an empty string otherwise.

*By @raxyte.*

### Read Decimal128 values from Parquet files

The `read_parquet` operator now reads Arrow `Decimal128` values as strings by default. Set `decimal_format="float"` to read them as potentially lossy real values instead. Decimal values in maps remain unsupported in float mode.

*By @raxyte.*

### Rebuilds no longer mistake page cache for used memory

Rebuilds running in a memory-limited cgroup now make full progress per batch instead of abandoning most of the partitions they selected.

A node computed the memory available to it from the cgroup's limit minus its current charge. Under cgroup v2 that charge includes clean page cache, so reading files inflated it even though the kernel reclaims those pages on demand. Rebuilds read every partition they merge, so they appeared to exhaust their own memory budget and stopped early, logging:

```text
stops loading transform input before partition <uuid> after 12 partition(s);
live memory budget is full
```

Reclaimable page cache and reclaimable slab no longer count towards a cgroup's consumed memory. Memory that needs writeback or swap first — dirty pages, pages under writeback, and shared memory — still counts. Readings taken from `/proc/meminfo` were already correct and are unchanged.

Because rebuilds now see the memory they actually have, each batch admits more partitions and takes correspondingly longer to complete. Raise `tenzir.automatic-rebuild` to run more batches concurrently and shrink each one.

*By @tobim and @claude.*
