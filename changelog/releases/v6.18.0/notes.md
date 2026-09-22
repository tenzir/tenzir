Configured pipeline parallelism now accelerates asynchronous transforms and supported competing-consumer sources, increasing throughput for DNS, context, Python, AMQP, and Google Cloud Pub/Sub workloads. This release also restores the SentinelOne Data Lake operators and adds recursive value search with the new search function.

## 🚀 Features

### Experimental warning suppression with quiet

The experimental `quiet` operator suppresses runtime warnings from a nested pipeline. Use it when parsing failures are expected and you handle the resulting null values yourself:

```tql
from {time: "2026-09-09T14:30:00Z"}, {time: "unknown"}
quiet {
  ts = time.parse_time("%Y-%m-%dT%H:%M:%SZ")
}
if ts == null {
  ts = now()
}
```

Errors and compilation diagnostics remain visible. Warnings outside the nested pipeline are unaffected.

*By @zedoraps.*

## 🔧 Changes

### Parallel competing-consumer sources

Pipelines configured with parallelism can now run multiple `from_google_cloud_pubsub` and `from_amqp` consumers when their broker configuration safely distributes messages between them. AMQP configurations that create independent consumers or reject additional consumers remain single-instance.

*By @aljazerzen.*

### Parallel execution for asynchronous transforms

The `dns_lookup`, `context_enrich`, `each`, and `python` operators now use the configured pipeline parallelism. Independent input batches run concurrently across operator instances, improving throughput for DNS requests, context lookups, child pipelines, and Python processing.

Python programs that retain state or perform side effects now run once per parallel operator instance. Use a parallelism degree of one when a program requires one continuous subprocess session.

*By @aljazerzen.*

### Position-independent static Linux binaries

Static Linux binaries now support address-space randomization for the executable itself.

*By @tobim.*

### Removal of the legacy non-neo executor

Tenzir now runs all pipelines with the current executor. The legacy non-neo executor and its compatibility-only operator implementations have been removed.

Pipelines using supported operators continue to run without changes. Operators that were available only through the legacy executor must be migrated to their current equivalents before upgrading.

*By @aljazerzen.*

### Search function for recursive value matching

The `search` function replaces `contains` for recursive value searches in records, lists, and scalar values. The matching behavior and the `exact` and `ignore_case` options are unchanged:

```tql
from {user: {name: "Alice"}}
found = search(user, "alice", ignore_case=true)
```

The old `contains` name still works but emits a deprecation warning. Replace `contains(input, target)` with `search(input, target)`, or `input.contains(target)` with `input.search(target)`.

*By @mavam.*

## 🐞 Bug fixes

### Input context for JSON UTF-8 errors

JSON parsing errors caused by invalid UTF-8 now show the surrounding input with the invalid bytes escaped, including when reading HTTP responses with `from_http`.

*By @tobim.*

### Reject source-position if statements

An `if` statement at the beginning of a pipeline now reports an error instead of terminating with an internal assertion failure.

*By @aljazerzen.*

### Restored SentinelOne Data Lake operators

The `from_sentinelone_data_lake` and `to_sentinelone_data_lake` operators are available again in packaged builds. Previously, these operators were missing, preventing pipelines from reading from or writing to SentinelOne Data Lake.

*By @mavam.*
