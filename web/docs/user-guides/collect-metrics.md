---
sidebar_position: 6
---

# Collect metrics

:::note Minimal overhead
Collecting metrics is optional and incurs minimal overhead. We recommend
enabling the accountant unless disk space is scarce or every last bit of
performance needs to be made available to other components of Tenzir.
:::

Tenzir keeps detailed track of system metrics that reflect runtime state, such
as ingestion performance, query latencies, and resource usage.

Components send their metrics to a central *accountant* that relays the
telemetry to a configured sink. The accountant is disabled by default and waits
for metrics reports from other components. It represents telemetry as regular
`tenzir.metrics` events with the following schema:

```yaml
metrics:
  record:
    - ts: timestamp
    - version: string
    - actor: string
    - key: string
    - value: string
    - metadata:
        map:
          key: string
          value: string
```

The `ts` field is always displayed in Coordinated Universal Time (UTC) without a
timezone offset. In case you want to correlate metrics data with a Tenzir log
messages you need to add the local timezone offset to arrive at the correct time
window for the matching logs.

The `version` field is the version of Tenzir.

## Enable metrics collection

Enable the accountant to collect metrics collection in your configuration:

```yaml
tenzir:
  enable-metrics: true
```

Alternatively, pass the corresponding command-line option when starting a Tenzir
node:

```bash
tenzir-node --enable-metrics
```

## Write metrics to a file or UNIX domain socket

Tenzir also supports writing metrics to a file or UNIX domain socket (UDS). You
can enable them individually or at the same time:

```yaml
tenzir:
  metrics:
    # Configures if and where metrics should be written to a file.
    file-sink:
      enable: false
      real-time: false
      path: /tmp/tenzir-metrics.log
    # Configures if and where metrics should be written to a socket.
    uds-sink:
      enable: false
      real-time: false
      path: /tmp/tenzir-metrics.sock
      type: datagram # possible values are "stream" or "datagram"
    # Configures if and where metrics should be written to Tenzir itself.
    self-sink:
      enable: false
```

Both the file and UDS sinks write metrics as NDJSON and inline the `metadata`
key-value pairs into the top-level object. Tenzir buffers metrics for these
sinks to batch I/O operations. To enable real-time metrics reporting, set the
options `tenzir.metrics.file-sink.real-time` or
`tenzir.metrics.uds-sink.real-time` respectively in your configuration file.

:::tip Self Sink ❤️ Pipelines
The self-sink routes metrics as events into Tenzir's internal storage engine,
allowing you to work with metrics using Tenzir's pipelines. The schema for the
self-sink is slightly different, with the key being embedded in the schema name:

```yaml
tenzir.metrics.<key>:
  record:
    - ts: timestamp
    - version: string
    - actor: string
    - key: string
    - value: string
    - <metadata...>
```

Here's an example that shows the start up latency of Tenzir's stores,
grouped into 10 second buckets and looking at the minimum and the maximum
latency, respectively, for all buckets.

```bash
tenzir-ctl export json '#schema == "tenzir.metrics.passive-store.init.runtime"
  | select ts, value
  | summarize min(value), max(value) by ts resolution 10s'
```

```json
{"ts": "2023-02-28T17:21:50.000000", "min(value)": 0.218875, "max(value)": 107.280125}
{"ts": "2023-02-28T17:20:00.000000", "min(value)": 0.549292, "max(value)": 0.991235}
// ...
```
:::
