This release resolves several node reliability issues and improves summarize throughput on interleaved-key inputs. It also adds the map_keys function and changes hive-partitioned filesystem sinks to omit partition fields from written files.

## 🚀 Features

### Add ingress/egress metrics to more operators

The `to_sentinelone_data_lake`, `to_azure_log_analytics`, `to_snowflake`, `from_sentinelone_data_lake`, `serve`, `serve_http`, and `serve_tcp` operators now report bytes and events metrics.

*By @IyeOnline in #6401.*

### Map record keys

The new `map_keys` function renames the top-level fields of a record by applying a lambda to each field name. This makes workflows like lowercasing HTTP header names possible with `request = request.map_keys(key => key.to_lower())`.

*By @raxyte in #6395.*

### Upper bounds for `assert_throughput`

The `assert_throughput` operator now accepts an optional `max_events` argument to warn when a pipeline exceeds an expected throughput range.

*By @raxyte in #6399.*

## 🔧 Changes

### Faster summarize on interleaved-key inputs

The `summarize` operator is significantly faster on inputs where consecutive events belong to different groups (for example, a log file replayed in a loop with interleaved source/destination pairs). Previously, aggregation state was updated once per group-key *transition*, which on interleaved data degenerates to one update per row. Each update pays the full cost of evaluating the aggregation expression, so throughput scaled linearly with the number of transitions rather than the number of distinct groups.

Aggregations are now updated exactly once per distinct group per incoming batch, regardless of how many times the group key changes within that batch. A measured benchmark with 1.2 million interleaved-key events and 10 aggregations showed a 31× throughput improvement (~17 k events/s → ~520 k events/s).

*By @aljazerzen and @claude in #6412.*

### Hive-partitioned filesystem sinks drop partition columns

Filesystem sink operators that use `partition_by` now omit the partition fields from the written payload. This is a breaking change for pipelines that expected those fields inside each written file. The values remain encoded in the hive-style directory components, such as `region=us/`, so readers that recover partition columns from the path no longer see duplicate fields.

*By @raxyte in #6400.*

## 🐞 Bug fixes

### BITZ streams from neo pipelines

Legacy `read_bitz` pipelines no longer report an unexpected internal error when they receive BITZ data produced by a neo-executed pipeline over TCP. This restores mixed-executor BITZ forwarding patterns such as `from "tcp://..." { read_bitz } | publish "..."`.

*By @tobim and @codex in #6397.*

### Catalog lookups after schema removal

Tenzir nodes no longer terminate with an internal error when catalog maintenance removes the last partition for a schema while lookups continue. Queries now treat the removed schema as absent.

*By @tobim and @codex in #6411.*

### Fix confusing operator names in metrics

Operator metrics and profiling no longer report demangled C++ type names such as `Discard<tenzir::table_slice>`. Instead, they use the operator's name as it appears in TQL, for example `discard`.

*By @aljazerzen in #6402.*

### Pipelines acknowledge stop requests while draining

Fixes a problem where stopping or updating a running pipeline would render a node unresponsive and report `pipeline manager request ... timed out` error messages. The pipeline manager now acknowledges the request immediately, remembers that the pipeline is still draining, and finishes stopping (and, for definition updates, restarting) in the background once the executor has exited.

*By @aljazerzen in #6392.*

### Serve and live export shut down promptly

Stopping a pipeline that ends in `serve` no longer waits for the entire buffer to drain, which previously made graceful shutdown slow or made it hang indefinitely when a large backlog was buffered with no client draining it. The buffered data is now dropped and pending requests are released so the operator exits immediately.

Live exports with `retro=true` no longer ignore graceful shutdown. Previously the pipeline could enter an indefinite live-wait after the retrospective backlog drained and never terminate.

*By @aljazerzen in #6403.*
