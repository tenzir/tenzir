Tenzir now supports the Splunk HEC acknowledgement protocol, preserving delivery guarantees across node restarts, and decodes raw Avro data with the new read_avro operator and parse_avro function. This release also branches events up to 9x faster and extends parallel execution with full parallelism options and independent Google SecOps output instances.

## 🚀 Features

### `parallel` accepts all parallelism options

The `parallel` operator now accepts the same options as the `// parallelism` directive. Use `fuse` to control how operator-to-operator channels inside the block are wired, and `limit_partitions` to cap the degree of keyed operators such as `summarize` and `deduplicate`:

```tql
parallel 8, fuse="none", limit_partitions=2 {
  where y > 100
  summarize g, total=sum(x)
}
```

`fuse` takes `none`, `parallel`, or `all`. Both options default to the value of the enclosing parallelism scope, so a nested `parallel` no longer resets them.

*By @aljazerzen.*

### Parallelize `to_google_secops`

The `to_google_secops` operator now runs as multiple instances when the pipeline is parallelized. Every instance batches, authenticates, and sends independently, so `parallel` bounds the in-flight requests per instance.

*By @aljazerzen.*

### Read and parse raw Avro data

The new `read_avro` operator decodes streams of concatenated raw Apache Avro binary datums.

The new `parse_avro` function decodes one raw Avro datum from each `blob` or `string` value. Both APIs require the writer schema through the `schema` argument. They decode raw binary data rather than Avro Object Container Files.

*By @raxyte.*

### Splunk HEC acknowledgements

The `accept_splunk` operator now supports the HEC acknowledgement protocol:

```tql
accept_splunk hec_token=secret("splunk-hec-token"), ack=true
```

HEC clients receive an `ackId` for each accepted request and can query its status through `/services/collector/ack`. An acknowledgement becomes ready after a checkpoint containing the complete request commits, so restored pipelines can preserve delivery guarantees across node crashes. IDs remain queryable for 10 minutes by default, making retries safe while keeping abandoned requests from exhausting the acknowledgement limit.

*By @mavam.*

## 🔧 Changes

### Faster event branching

Pipelines that branch events with `if` now spend less time copying them — up to 9x more throughput on a batch whose matching events are contiguous.

When the condition holds for a few contiguous stretches of a batch, the branches are handed views of that batch instead of freshly built copies. This is the common case for conditions over a sorted field, a time window, or a schema discriminator:

```tql
if severity >= 4 {
  to_splunk "https://splunk.example.com:8088"
} else {
  to_hive "s3://bucket/archive"
}
```

A batch whose matching events fall into at most four runs is now split without copying. Beyond four the previous behavior applies, because each view becomes a message of its own and past that point the messages cost more than the copy saves.

Fragmented conditions, which still have to copy, gain about 20% from a faster scan over the condition.

Keyed and round-robin fan-out to parallel workers no longer does per-worker bookkeeping that it went on to discard when the routing key was already grouped.

*By @aljazerzen and @claude.*

## 🐞 Bug fixes

### Python support in Nix-built container images

Nix-built container images now include the `python3` executable required by Docker-based benchmark runs and Python-enabled pipelines.

*By @tobim.*

### Renamed builtin operators work on the legacy executor again

The v6.13.0 rename of module-qualified builtin operators to flat names split their two implementations across different registry entries: the legacy implementation stayed under the old spelling, while the flat name only carried the new-IR implementation. As a result, pipelines running on the legacy executor—such as deployed pipelines restored by the node on startup—failed with `this operator can only be used with the new IR` for `context_*`, `ocsf_cast`, `ocsf_derive`, `ocsf_trim`, `package_*`, and `pipeline_*` operators, under both the old and the new spelling.

Both halves now register under the flat name again, so these operators work on both execution paths with either spelling.

*By @tobim.*

### Store sizes for rebuilt and compacted partitions

The `partitions` operator now reports the correct `diskusage` and `store` for every partition that a rebuild or a compaction produced.

*By @jachris.*
