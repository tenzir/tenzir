Tenzir now lets you build, store, merge, and compare statistical models in TQL to detect distribution drift across pipelines and time windows. The release also adds session windows, bounded event-time reordering, preceding-event access, native Splunk HEC ingestion, and more self-contained container and Python deployments.

## 🚀 Features

### Bounded event-time reordering

The new `reorder` operator puts events into timestamp order while buffering a bounded span of event time. For example, this pipeline receives the event at two seconds before the event at one second:

```tql
from {id: "first", time: 0s.from_epoch()},
     {id: "third", time: 2s.from_epoch()},
     {id: "second", time: 1s.from_epoch()}
reorder on=time, tolerance=2s
```

It produces:

```tql
{id: "first", time: 1970-01-01T00:00:00Z}
{id: "second", time: 1970-01-01T00:00:01Z}
{id: "third", time: 1970-01-01T00:00:02Z}
```

The operator preserves arrival order for equal timestamps and flushes its remaining ordered events when finite input ends. Events that arrive too late or have an invalid timestamp are dropped with a warning. Place `reorder` inside `group` when each key needs an independent event-time watermark.

*By @mavam.*

### Lag operator

Many detections need to compare an event with the preceding event. The new `lag` operator adds that earlier value or complete event to the current event while preserving the input stream. Compose it with `group` to maintain an independent history for each user, host, process, or session:

```tql
from \
  {sequence: 1, user: "alice", location: "Berlin"},
  {sequence: 2, user: "bob", location: "Paris"},
  {sequence: 3, user: "alice", location: "London"}
group user {
  lag value=location, into=previous_location
}
sort sequence
drop sequence
```

```tql
{user: "alice", location: "Berlin", previous_location: null}
{user: "bob", location: "Paris", previous_location: null}
{user: "alice", location: "London", previous_location: "Berlin"}
```

The third event now contains Alice's last observed location, while Bob's history remains independent. The final sort restores the original order for this bounded example; `group` preserves order within each user but not between users. A login detection can attach the preceding location and timestamp for each user, calculate the distance and elapsed time, and flag impossible travel or possible account sharing. The same pattern can detect state transitions for accounts, processes, and sessions or calculate deltas between adjacent metrics.

Set `offset` to select an earlier event. Omit `value` to attach the complete preceding event. Put `reorder` before `lag` when event-time input can arrive out of order. Without an enclosing window, `group` retains one subpipeline and `lag` retains `offset` values per key until the input ends. Place grouped lag operations inside `window` when its boundaries fit the detection semantics. Unlike a trailing count window, `lag` doesn't replay retained events through a subpipeline for every input event.

*By @mavam.*

### Mergeable statistical models and distribution comparisons

TQL now provides statistical models that you can inspect, store as ordinary records, merge across pipelines, and compare for distribution drift. The new aggregation functions create fixed-width numeric [histograms], exact categorical [frequency tables], approximate [t-digests], and approximate distinct counts with [HyperLogLog]:

```tql
summarize \
  size_distribution = histogram(size, bins=20, width=500.0),
  action_counts = frequency_table(action),
  latency_distribution = tdigest(latency_ms),
  distinct_users = hll(user_id)
```

Each model is a versioned TQL record, so you can send it through a pipeline or store it in a lookup table like other data. Use `model_merge` to combine models from separate windows, nodes, or stored records.

The models include dedicated query functions:

- `histogram_bucket` returns the bucket and count for a numeric value.
- `frequency_table_count` returns the exact count for a categorical value.
- `tdigest_quantile` and `tdigest_cdf` query an approximate distribution.
- `hll_cardinality` estimates the number of distinct values.

You can compare compatible histogram and frequency-table models with [Jensen-Shannon divergence][jensen-shannon]. You can compare t-digests with [Kolmogorov-Smirnov] or [Wasserstein] distance:

```tql
from {
  baseline_sizes: histogram([100.0, 200.0, 300.0], bins=4, width=100.0),
  current_sizes: histogram([100.0, 700.0, 800.0], bins=4, width=100.0),
  baseline_latency: tdigest([10.0, 12.0, 15.0]),
  current_latency: tdigest([10.0, 40.0, 50.0]),
}
select \
  size_drift = model_divergence(baseline_sizes, current_sizes, method="jensen_shannon"), \
  latency_shift = model_distance(baseline_latency, current_latency, method="wasserstein")
```

This produces:

```tql
{
  size_drift: 0.46209812037329684, // Investigate the newly observed large sizes
  latency_shift: 21.0, // Alert if this shift exceeds your latency tolerance
}
```

New generic distribution functions also operate directly on lists:

- `jensen_shannon` compares aligned weight vectors.
- `ecdf` evaluates an exact empirical CDF.
- `kolmogorov_smirnov` compares numeric or temporal samples.
- `wasserstein` compares numeric or temporal samples. For duration and timestamp samples, it returns a duration.

```tql
from {
  divergence: jensen_shannon([10, 20, 0], [5, 20, 5]),
  probability: ecdf([1, 2, 2, 4], 2),
  sample_distance: kolmogorov_smirnov([1, 2, 3], [2, 3, 4]),
  time_shift: wasserstein([1s, 2s, 3s], [2s, 3s, 4s]),
}
```

*By @mavam.*

### Native Splunk HEC ingestion

Tenzir can now receive Splunk HTTP Event Collector (HEC) traffic without a Fluent Bit bridge. The new `accept_splunk` operator accepts event and raw requests, gzip compression, HEC token authentication, and TLS:

```tql
accept_splunk hec_token=secret("splunk-hec-token")
publish "splunk"
```

The listener defaults to the standard HEC port `8088`, preserves HEC metadata and raw request boundaries, and validates complete batches before emitting events.

*By @mavam.*

### Session windows for inactivity-defined event groups

The `window` operator can now group events into sessions that close after a configured period of inactivity. Consecutive events remain in the same session while their time difference does not exceed `gap`, even when the total session duration grows beyond that gap:

```tql
group host {
  window gap=5min, on=time, tolerance=30s {
    summarize events=count()
  }
}
```

Omit `on` for processing-time sessions. Event-time sessions accept bounded out-of-order input through `tolerance` and can use `idle_timeout` to close after wall-clock inactivity. An optional `size` caps a session by either duration or event count:

```tql
window gap=5min, size=24h, on=time { … }
window gap=5min, size=1000, on=time { … }
```

A session accepts one of these caps at a time.

The operator delivers a session's events to its subpipeline in batches and emits subpipeline output in session order. A later session cannot overtake an earlier one when their subpipelines finish concurrently. A subpipeline that stops early, such as one starting with `head`, consumes the remaining events of its session; a later event within the gap then opens a new session. Checkpointing closes the open session, so a pipeline restored from a checkpoint starts sessions fresh.

*By @mavam.*

## 🔧 Changes

### Fuse channels by default

By default, most operators will now not run concurrently. This change will significantly reduce operating memory usage and memory usage under full backpressure. It can also lead to 40% lower maximum pipeline troughput.

Pipelines that need the higher throughput ceiling can opt back into it with an explicit directive, e.g.:

```tql
// parallelism: 4
```

*By @aljazerzen.*

### Nix-built container images

The `tenzir/tenzir`, `tenzir/tenzir-node`, and `tenzir/tenzir-demo` images are now built from the same Nix expression as the `-slim` images instead of from a Debian-based Dockerfile. The images keep their runtime contract - the unprivileged `tenzir` user with uid and gid 999, the state, cache, and log directories under `/var`, `TENZIR_ENDPOINT=0.0.0.0`, and the `/var/lib/tenzir` working directory - but their dependency closure and their reported build metadata now match the `-slim` images.

All images read configuration from `/opt/tenzir/etc/tenzir`, the location the Debian-based images used, so a `tenzir.yaml` or a `packages/` directory mounted there is picked up. The Nix-built images previously derived that path from their Nix store prefix, where nothing can be mounted, and silently used no configuration at all.

The `-slim` images gain that runtime contract in return: they no longer run as `root`, they ship pre-created state directories, `/tmp` is writable, and HTTPS works - their trust store was installed under a name the HTTP client does not look for, so every TLS connection failed to load its CA paths.

The `tenzir/tenzir-demo` image works again. It now ships the demo package and two pre-configured pipelines that import the Zeek and Suricata demo feeds when the node starts. Previously the image tried to install a package that no longer exists in the library, so a demo node came up without data.

All images are roughly 700 MB smaller. Their bundled Python runtime environment no longer contains an unused Ansible installation that a packaging mistake had pulled in.

The `tenzir/tenzir-deps` image is no longer published. It only ever carried the Debian build dependencies, which the Nix build does not use.

*By @tobim.*

### Offline Python operator

Basic usage of the `python` operator now works without an internet connection. All packages bundle the operator's complete Python dependency set as wheels, so setting up the operator's environment no longer downloads anything from PyPI. The container images additionally pin the operator to their bundled Python interpreter, which previously had to be fetched from the network on first use.

The operator's environment now always matches the bundled wheels: packages record the Python version the wheels were built for, and the operator provisions a matching interpreter on systems whose default Python differs. When no matching interpreter is available and none can be downloaded, the operator falls back to the interpreter it finds and resolves its dependencies from PyPI, emitting a warning. Setting `UV_PYTHON` still overrides the interpreter choice.

Passing extra packages via the operator's `requirements` option still requires network access to fetch them. Such packages now work inside the container images, which previously lacked the system libraries that prebuilt Python wheels expect.

Despite the bundled wheels, the container images shrink by roughly 400 MB: the Python environment they carried existed only to back the operator's dependencies and is gone now that the wheels are self-sufficient.

The `tenzir` wheel on PyPI keeps its size: it ships only the portable subset of the bundle, and the operator resolves the binary dependencies from PyPI - a pip-installed node runs on a machine with internet access by definition.

*By @tobim.*

## 🐞 Bug fixes

### `where` on lists no longer slows down with batch size

Filtering a list with `xs.where(x => …)` used a bitmap with linear-time random access to select the matching elements, making the function quadratic in the number of list elements per batch. Pipelines got dramatically slower as their batches grew: filtering 262144 events with a five-element list at a batch size of 65536 took 21 s and now takes 2.2 s.

*By @aljazerzen.*

### Google Cloud Logging sink in the static builds

The `to_google_cloud_logging` operator is now part of the static builds, which includes the `-slim` container images and the packages on [get.tenzir.app](https://get.tenzir.app). Their `google-cloud-cpp` dependency was built without the `logging` API, so the plugin disabled itself at configure time with nothing but a warning to show for it.

*By @tobim and @claude.*

### Rebuilder crash after a failed parallel rebuild batch

The rebuilder no longer crashes with `called Option::operator-> on a None value` when one of its parallel workers fails mid-run. Previously, a single failing batch—for example, a partition transform aborting because its memory budget was exhausted—ended the run while sibling workers still had follow-up work queued, and the next queued message brought down the rebuilder until the node was restarted. Automatic rebuilds silently stopped for the remainder of the node's lifetime. The stale follow-up work is now discarded and the rebuilder stays available for subsequent runs.

*By @tobim.*

### Reliable shutdown for malformed BITZ input

Tenzir no longer crashes while shutting down a pipeline that reads malformed BITZ data from standard input.

*By @tobim.*

### Retry temporary HTTP connection failures

HTTP-based pipelines with retries configured no longer fail immediately when a connection is temporarily unavailable. This includes SQS, Splunk exports, and CloudWatch Live Tail.

*By @raxyte.*

[frequency tables]: https://en.wikipedia.org/wiki/Frequency_distribution
[histograms]: https://en.wikipedia.org/wiki/Histogram
[hyperloglog]: https://en.wikipedia.org/wiki/HyperLogLog
[jensen-shannon]: https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence
[kolmogorov-smirnov]: https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
[t-digests]: https://arxiv.org/abs/1902.04023
[wasserstein]: https://en.wikipedia.org/wiki/Wasserstein_metric
