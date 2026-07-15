This release introduces the `fork_merge` operator for fanning out subpipelines over the same input and merging their outputs into one stream. It also improves the performance of the Tenzir Platform and restores raw-log ingestion for Google SecOps.

## 🚀 Features

### Fan out with the fork_merge operator

The new `fork_merge` operator runs multiple subpipelines on the same input stream and merges their outputs back into a single stream. Each branch receives a copy of every event, and the results are interleaved downstream:

```tql
subscribe "in"
fork_merge {
  summarize a=sum(bytes)
}, {
  summarize b=count()
}, {
  summarize c=max(duration)
}
publish "out"
```

Unlike `fork`, whose subpipeline must end in a sink and which forwards its input unchanged, `fork_merge` lets you fan out independent computations and rejoin them. Every branch is an events-to-events transformation.

*By @aljazerzen and @claude in #6436.*

### Force stop action for pipelines

Pipelines now support a `force-stop` action that terminates a pipeline immediately instead of waiting for in-flight data to drain.

A regular `stop` moves a running pipeline into the `stopping` state and lets it drain gracefully, which can take up to the configured `tenzir.shutdown-grace-period`.

Sending `force-stop` to a pipeline that is already `stopping` cancels the grace period and kills it immediately.

*By @aljazerzen and @claude in #6442.*

### Improved Platform Performance Support

In order to improve the performance of the Tenzir Platform, we made a few internal changes to the Tenzir Node. Notably the `/serve-multi` endpoint received several improvements that reduce latency and overhead when driving the frontend, making pipeline output fetching more responsive.

This does not requires any user action. We recommend that you do not manually use the `serve` operator or endpoints.

*By @lava in #6373.*

## 🐞 Bug fixes

### Cleaner source builds on macOS

Source builds on macOS no longer print avoidable warnings from platform-specific code, fmt 12 integration, or bundled dependencies. This makes new compiler diagnostics easier to spot.

*By @mavam and @codex in #6441.*

### Google SecOps unstructured ingestion support

The `to_google_secops` operator can once again forward raw logs without parsing their timestamps by selecting the supported Ingestion API:

```tql
from {raw_log: "<134>1 2026-07-14T09:00:00Z host app - - - message"}
to_google_secops api="ingestion",
  log_text=raw_log,
  log_type="CUSTOM_JSON",
  private_key=secret("google-private-key"),
  client_email=secret("google-client-email"),
  customer_id=secret("google-customer-id")
```

With `api="ingestion"`, `log_entry_time` is optional so that Google SecOps can derive the timestamp from the raw log. The `api="import"` path remains the default for compatibility with Tenzir 6.2, and UDM events and entities continue to use the Import API.

*By @mavam and @codex in #6446.*

### Reset grouped summarize state after periodic emission

The `summarize` operator no longer retains inactive group keys when using periodic emission in `reset` mode. Previously, every interval emitted one event for every group seen since the pipeline started, including inactive groups with reset aggregate values. This caused output batches and import metrics to grow over time. Each interval now contains only groups that received events during that interval.

*By @raxyte in #6435.*
