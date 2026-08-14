This release expands detection engineering with full Sigma v2.1 support and YARA-X findings while adding richer streaming analytics, windows, and parallel execution.

## 🚀 Features

### Add a `merge` operator

The new `merge` operator merges the output of a source subpipeline into the main event stream. It is the dual of `fork`: where `fork` attaches an additional sink that consumes a copy of the input, `merge` attaches an additional source that contributes events to the output.

```tql
subscribe "primary"
merge {
  subscribe "secondary"
}
publish "combined"
```

*By @aljazerzen.*

### Dynamic string replacement

The `replace` function now accepts dynamic patterns and replacements:

```tql
message = message.replace(user.name, "<redacted>")
```

Literal `replace` calls with an empty pattern leave the subject unchanged.

*By @zedoraps and @codex.*

### Exponential, logarithmic, and power functions

TQL now provides `exp`, `log`, and `pow` for exponential and logarithmic calculations. `log` uses the natural base by default and accepts an optional base for arbitrary logarithms:

```tql
from {
  natural: exp(1).log(),
  binary: log(8, 2),
  power: pow(2, 10),
}
```

```tql
{
  natural: 1.0,
  binary: 3.0,
  power: 1024,
}
```

*By @mavam.*

### Final aggregate annotations for events

The `summarize` operator can now attach final aggregate values to every event in a finite input. Set `output: "events"` to preserve the original evidence while adding the completed statistics for each group:

```tql
from {user: "alice", value: 10},
     {user: "alice", value: 20},
     {user: "bob", value: 5}
summarize user, avg=mean(value), samples=count(),
  options={output: "events"}
```

This output policy provides the TQL counterpart to Splunk's `eventstats` and composes with `window` for bounded populations. It is separate from `mode`, which controls whether aggregate state resets or accumulates across emission boundaries.

*By @mavam.*

### Full Sigma v2.1 detection support

The `sigma` operator now implements the Sigma v2.1.0 detection rule surface for the default `sigma` taxonomy, except placeholder expansion with `expand`. Rules with custom taxonomies or `expand` are rejected with an explicit diagnostic. Keyword selections match every string-valued leaf of an event, including strings nested in records and lists.

Modifier support now includes `exists`, `cased`, `neq`, `windash`, `fieldref`, the UTF-16 encoding modifiers, the `re` sub-modifiers `i`/`m`/`s`, and the time-part modifiers `minute` through `year`. Previously, several modifiers were silently misinterpreted; rules relying on them now match correctly.

Conditions can now be lists of OR-linked queries. Quantifiers such as `all of selection_*` combine matching selections without rewriting their internal logic, matching the behavior of pySigma.

Sigma wildcards now treat all regex metacharacters literally and match across newlines. For example, a value like `a+b` no longer accidentally matches `ab`. Field names containing dots deterministically prefer an exact top-level key before nested traversal.

*By @mavam.*

### Geographic distance calculations

The new `geo_distance` function calculates the surface distance in meters between two longitude/latitude pairs. It uses a fast spherical calculation by default and can account for the WGS-84 spheroid when you need greater accuracy:

```tql
from {
  distance_m: geo_distance(13.405, 52.52, -0.1276, 51.5072),
  precise_distance_m: geo_distance(
    13.405,
    52.52,
    -0.1276,
    51.5072,
    spheroid=true,
  ),
}
```

```tql
{
  distance_m: 931561.8960448167,
  precise_distance_m: 934514.4909447534,
}
```

Invalid, non-finite, and null coordinates produce `null`.

*By @mavam.*

### Named path and rules arguments for the sigma operator

The `sigma` operator now accepts rule sources through the named `path=` and `rules=` arguments.

Use `path=` with a file, a directory, or a list containing both:

```tql
sigma path=["rules/detection.yml", "rules/windows/"]
```

The operator searches directories recursively in deterministic order and loads a file only once when overlapping paths refer to it. Explicitly named files no longer need a `.yaml` or `.yml` extension. A rule file can also contain multiple YAML documents, which the operator loads as separate rules.

Use `rules=` to embed YAML directly in a pipeline. The argument accepts one string or a list of strings, and each string can contain a single rule or a multi-document YAML collection:

```tql
sigma rules=r#"
title: Detect an administrator login
detection:
  selection:
    user: administrator
  condition: selection
"#
```

The operator validates embedded rules while constructing the pipeline and does not access the filesystem for them. Empty and invalid embedded sources produce a diagnostic at the `rules=` argument.

Passing a path positionally remains supported for compatibility, but now emits a deprecation warning. Use `path=` instead. The `refresh_interval` argument applies only to filesystem-backed sources and cannot be combined with `rules=`. If a rule directory cannot be inspected during a refresh, the operator reports the failure at the `path=` argument and keeps the previously loaded rules. A rule source that no longer compiles also keeps its last valid version while rules from other sources continue to update. The operator reports each broken content revision once. Removing a file from a successfully inspected directory removes its rules.

*By @mavam.*

### Parallel pipeline execution

Pipelines can now run CPU-bound operators on multiple cores.

Until now, the only way to use more than one core inside a pipeline was to wrap a subpipeline in `parallel { … }`. This had problems with some operators like `summarize` and `deduplicate`, which would produces partial results for each parallel leg.

Now Tenzir parallelizes the individual operators of a pipeline, and it preserves their semantics while doing so.

Parallelism is off by default. Turn it on for a pipeline with a `// parallelism:` comment in its leading comment lines:

```tql
// parallelism: 4
from_file ...
where dest_port == 443
bytes = orig_bytes + resp_bytes
summarize dest_ip, total=sum(bytes)
to_clickhouse ...
```

Here, `where`, the assigment and the `summarize` would run with 4 parallel instances. Each `summarize` instance is reponsible for aggregation of a fourth of all possible `dest_ip`.

Parallelism gives up ordering: events overtake each other as they flow through concurrent instances, so a parallel pipeline may emit them in a different order than it received them. Use `sort` if you need a specific order downstream, and leave parallelism disabled when the incoming order carries meaning.

The `tenzir` binary also accepts `--parallelism` with the same values, which applies to pipelines that carry no comment.

*By @aljazerzen.*

### Processing-time, count, and trailing windows

The `window` operator now supports processing-time, count-based, and event-anchored trailing windows in addition to fixed event-time windows.

Omit `on` for wall-clock processing-time windows, use an unsigned `size` for event-count windows, or set `trailing=true` to run a bounded trailing window. By default, every input event fires a trailing window:

```tql
window size=5min, trailing=true, on=ts {
  summarize rolling_bytes=sum(bytes)
  this = {...$window.event, rolling_bytes: rolling_bytes}
}
```

Trailing windows expose the triggering event through `$window.event`, which enables bounded `streamstats`-style enrichment with arbitrary subpipelines. Combine `trailing=true` with `every` to sample retained history at a lower count or duration cadence:

```tql
window size=10_000, every=100, trailing=true {
  summarize p99=quantile(latency, 0.99)
  this = {...$window.event, p99: p99}
}
```

The optional `trigger` expression selects which events fire a trailing window. Every event still enters the retained history, but only events for which `trigger` evaluates to `true` run the subpipeline. This avoids replaying history for events you do not care about:

```tql
window size=10min, trailing=true, on=ts, trigger=status_id == 1 {
  summarize failures=count_if(status_id, x => x == 2)
  where failures >= 5
  this = {...$window.event, prior_failures: failures}
}
```

For slightly out-of-order event-time streams, set `tolerance` to the maximum expected lateness. The operator reorders events within this bound before evaluating each trailing window and reports later events through the existing late-event warning:

```tql
window size=10min, trailing=true, on=ts, tolerance=30s {
  summarize failures=count_if(status_id, x => x == 2)
}
```

The reorder buffer can retain up to `tolerance` worth of events in addition to the trailing `size`.

*By @mavam.*

### Sigma global filter rules

The `sigma` operator now supports Sigma v2.1 global filters, which let you tune multiple detection rules without modifying the original rules.

For example, the following multi-document YAML file detects a suspicious process but suppresses matches from known administrator accounts:

```yaml
title: Suspicious process
id: 6f3e2987-db24-4c78-a860-b4f4095a7095
logsource:
  category: process_creation
detection:
  selection:
    Image|endswith: '\evil.exe'
  condition: selection
---
title: Ignore administrator accounts
logsource:
  category: process_creation
filter:
  rules:
    - 6f3e2987-db24-4c78-a860-b4f4095a7095
  administrator:
    User|startswith: adm_
  condition: not administrator
```

A filter can target rules by `id` or `name`. Use `rules: any` instead of a list to apply a filter to every rule with a compatible log source. You can keep filters next to their target rules in multi-document YAML, place them in separate files, or pass them through the inline `rules=` option.

If a filter references an unknown or ambiguous rule, or its log source is incompatible with a target, the operator emits a warning and continues running the affected rules without that filter. When a filter becomes invalid during a hot reload, the operator keeps its last valid version active until you provide a working replacement.

*By @mavam.*

### Sigma processing metrics

The `sigma` operator now emits lightweight `tenzir.metrics.sigma` events for each processed batch. The metrics report the number of input events, rule evaluations, and matches, making it possible to monitor Sigma processing load and match rates.

*By @mavam.*

## 🔧 Changes

### Incremental summaries for event streams

The `summarize` operator can now add running aggregates to every input event or emit aggregates periodically in processing time. For example, this SPL query uses `streamstats` to compute a running byte sum and event count per user:

```spl
... | streamstats sum(bytes) AS running_bytes count AS events BY user
```

You can express the same unbounded running aggregates in TQL with cumulative event emission:

```tql
from {user: "alice", bytes: 10},
     {user: "bob", bytes: 5},
     {user: "alice", bytes: 20}
summarize running_bytes=sum(bytes), events=count(), user,
  options={mode: "cumulative"}
```

Per-event emission is the default when you set `mode` without `emit`. Set `emit` to a positive integer to emit after that many input events. For example, `emit: 1` emits every event, while `emit: 100` emits every 100th event and the final event of a partial interval.

For periodic output, set `emit` to a duration and choose whether each emission resets or retains aggregate state:

```tql
summarize count=count(), options={emit: 5s, mode: "reset"}
```

Timer intervals must be at least 10ms; smaller values for `emit` (and the deprecated `frequency`) are rejected.

The `frequency` option remains available temporarily and emits a migration warning. Update existing pipelines as follows:

| Previous options                              | Replacement                              |
| --------------------------------------------- | ---------------------------------------- |
| `options={frequency: 5s}`                     | `options={emit: 5s, mode: "reset"}`      |
| `options={frequency: 5s, mode: "reset"}`      | `options={emit: 5s, mode: "reset"}`      |
| `options={frequency: 5s, mode: "cumulative"}` | `options={emit: 5s, mode: "cumulative"}` |

The `"update"` mode is no longer supported.

*By @mavam.*

### Sigma matches become OCSF Detection Findings

The `sigma` operator now emits an OCSF 1.9.0 Detection Finding for every rule match instead of the previous `tenzir.sigma` record with `event` and `rule` fields.

For example, apply a rule and inspect the event, causal identifiers, matched fields, and condition trace:

```tql
from {user: "alice", action: "login"}
sigma rules=r#"
title: Alice login
detection:
  selection:
    user: alice
  condition: selection
"#
select event = evidences[0].data,
       identifiers = finding_info.traits,
       fields = evidences[1].sigma.fields,
       trace = evidences[1].sigma.trace
```

Each finding includes:

- The complete applied rule in `policy.data` and its normalized identity in `finding_info.analytic`.
- The rule's severity in `severity_id` and MITRE ATT&CK tags in `attacks`.
- The matching event in `evidences[0].data`.
- Causal search identifiers in `finding_info.traits` and positive matched field values in `observables`.
- Detailed matcher, case, polarity, and condition-trace provenance in a second evidence entry at `evidences[1].sigma`.

The OCSF format remains the default. To emit the previous lightweight output shape without constructing an OCSF finding, set `format="plain"`:

```tql
sigma path="rule.yaml", format="plain"
```

This emits a `tenzir.sigma` record with the original event in `event` and the matched rule in `rule`.

*By @mavam.*

### Stricter validation of Sigma rules

The `sigma` operator now validates rules before executing them and rejects invalid rules with actionable diagnostics instead of silently misinterpreting them. Rules using an unknown or unsupported modifier (for example `a|frobnicate: 1`) are now rejected instead of the modifier being silently ignored, conditions referencing an unknown search identifier report the identifier by name, and quantifier patterns such as `1 of selection_*` that match no search identifier are rejected instead of never matching. Rules declaring `sigma-version` with an unsupported major version now fail explicitly rather than being interpreted with older semantics.

*By @mavam.*

### YARA-X detection findings

The `yara` operator now uses YARA-X and emits one OCSF 1.9.0 Detection Finding for every matching rule by default. Findings include the applied rule, a readable and stable `yara:<namespace>:<identifier>` analytic identity, a unique finding identity, the scanned input's SHA-256 digest, and ordered match evidence with global count and encoded-size bounds. Set `format="plain"` to emit the original byte stream and native YARA rule metadata without constructing an OCSF finding.

This change removes the legacy positional syntax, `compiled_rules`, and the `yara.match` output. Specify exactly one rule source with `path=` or `rules=`:

```tql
from_file "suspicious.exe", mmap=true {
  yara path="/etc/tenzir/yara"
}
```

Use `rules=` for inline rules. New options control include directories, scan timeouts, maximum input size, and the maximum stored matches per pattern. Scans now time out after `1min` by default; set `timeout=` explicitly for workloads that need longer. The `vt` and `cuckoo` modules are unsupported because Tenzir does not provide their runtime data. YARA-X may also reject or interpret some legacy rules differently.

*By @mavam.*

## 🐞 Bug fixes

### `quantile` keeps its configuration across list rows

Calling `quantile` on lists no longer degrades to the minimum after the first row. The per-row evaluation reset the aggregation state between rows and incorrectly cleared the configured quantile along with it, so `xs.quantile(q=0.5)` returned the median for the first event and the minimum for every subsequent one.

The `quantile` aggregation now also participates in pipeline snapshots: restoring a checkpoint preserves the accumulated t-digest state instead of failing the restore.

*By @mavam and @claude.*

### Accurate CPU usage in operator profile metrics

The `cpu` field of the `operator_profile` metrics no longer reports inflated or negative values, and every event now carries the time span its counters cover.

`cpu` is the share of a CPU core an operator used since the previous sample. It was computed by dividing the consumed CPU time by the nominal sampling interval of one second rather than by the time that actually passed. Samples are taken by the pipeline itself, so they arrive late exactly when the pipeline is busy, and the reported usage grew with the delay. An operator that cannot use more than one core could report well above `100`. Operators that had already finished could report a negative value.

Each event now also has a `duration` field holding the measured time span its counters cover, so rates no longer have to assume that events arrive exactly one second apart:

```tql
metrics "operator_profile"
events_per_second = events_out / duration.count_seconds()
```

The window of the first sample of a pipeline now starts when the pipeline began executing. It previously started when the metrics collection happened to be scheduled, which inflated the first reported rates by however long that took.

*By @aljazerzen and @claude.*

### Correct parsing of platform-issued node tokens

Some valid platform-issued tokens previously prevented nodes from starting. Nodes now correctly parse these tokens.

*By @zedoraps and @codex.*

### Empty pipelines run as a no-op

Pipelines without any operators now run as a no-op instead of failing. Running a pipeline whose definition is empty — for example because every line is commented out — previously failed with `empty pipeline is not supported yet` on the command line, and with an internal error when the pipeline ran on a node:

```text
error: pipeline failed

error: unexpected internal error: assertion `chain.size() != 0` failed
```

Such pipelines now complete successfully without producing any output, which makes it safe to comment out the contents of an existing managed pipeline and run it again.

*By @aljazerzen and @claude.*

### Reading map columns without keys

Reading a map column whose rows are all null no longer fails with an internal error:

```text
error: unexpected internal error: assertion `r` failed
```

Tenzir represents maps as records, deriving the record's fields from the keys that the map contains. A column that holds no keys at all has no fields to derive from, which previously went unhandled. Such columns now read as an empty record that is null in every row. This most visibly affected `read_parquet`.

*By @zedoraps and @claude.*

### Safe record transforms on sliced input

The `select_matching()` and `drop_matching()` functions and the `unroll` operator no longer crash when they transform nested records from a sliced multi-row batch. These operations now preserve record values and nulls after slicing.

*By @mavam and @codex.*

### Separate Nix build tags from versions

The version operator now reports release versions without the Nix build identifier. The identifier remains available in the tag field.

*By @tobim.*

### Spurious unreachable warnings when deleting pipelines

Deleting or stopping pipelines no longer produces spurious `received down message for unknown pipeline: !! unreachable` warnings in the node log, and the affected pipelines are no longer marked as failed with a bogus `unreachable` diagnostic. Creating and deleting many pipelines in quick succession previously made these messages a steady stream of noise.

*By @aljazerzen and @claude.*

### Timely pipeline activity metrics

Pipeline activity metrics now reach the Tenzir Platform without waiting for the `tenzir.import-buffer-timeout` batching interval.

*By @tobim and @codex.*
