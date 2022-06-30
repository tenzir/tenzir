---
sidebar_position: 6
---

# Monitor

:::note Minimal overhead
Collecting metrics is optional and incurs minimal overhead. We recommend
enabling the accountant unless disk space is scarce or every last bit of
performance needs to be made available to other components of VAST.
:::

VAST keeps detailed track of system metrics that reflect runtime state, such
as ingestion performance, query latencies, and resource usage.

[Components](/docs/understand-vast/architecture/components) send their metrics
to a central *accountant* that relays the telemetry to a configured sink. The
accountant is disabled by default and waits for metrics reports from other
components. It represents telemetry as regular `vast.metrics` events with the
following schema:

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
timezone offset. In case you want to correlate metrics data with a VAST log
messages you need to add the local timezone offset to arrive at the correct time
window for the matching logs.

The `version` field is the version of VAST.

## Enable metrics collection

Enable the accountant to collect metrics collection in your configuration:

```yaml
vast:
  enable-metrics: true
```

Alternatively, pass the corresponding command-line option when starting VAST:
`vast --enable-metrics start`.

## Report metrics as ordinary events

By default, VAST reports metrics using the *self sink*, i.e., they are ingested
as `vast.metrics` events back into VAST. This allows them to be queried like
regular data. The self sink has the following configuration options:

```yaml
vast:
  metrics:
    self-sink:
      enable: true
      slice-size: 100
```

The following query operates on all metrics with the key `pcap-reader.recv`
that keeps track of a delta of received packets on all interfaces VAST listens
on. With JSON as export format, you can use `jq` to calculate the sum of these
values to get the total amount of packets seen on all interfaces.

```bash
vast export json '#type == "vast.metrics" && key == "pcap-reader.recv"' |
  jq -s 'map(.value | tonumber) | add'
```

## Write metrics to a file or UNIX domain socket

VAST also supports writing metrics to a file or UNIX domain socket (UDS). You
can enable them individually or at the same time:

```yaml
vast:
  metrics:
    # Configures if and where metrics should be written to a file.
    file-sink:
      enable: false
      real-time: false
      path: /tmp/vast-metrics.log
    # Configures if and where metrics should be written to a socket.
    uds-sink:
      enable: false
      real-time: false
      path: /tmp/vast-metrics.sock
      type: datagram # possible values are "stream" or "datagram"
```

Both sinks write metrics as NDJSON and inlines the `metadata` key-value pairs
into the top-level object. VAST buffers metrics for these sinks in order to
batch I/O operations. To enable real-time metrics reporting, set the options
`vast.metrics.file-sink.real-time` or `vast.metrics.uds-sink.real-time`
respectively in your configuration file.

## Reference

The following list describes all available metrics keys:

|Key|Description|Unit|Metadata|
|-:|-|-|-|
|`accountant.startup`|The first event in the lifetime of VAST.|constant `0`||
|`accountant.shutdown`|The last event in the lifetime of VAST.|constant `0`||
|`archive.rate`|The rate of events processed by the archive component.|#events/second||
|`arrow-writer.rate`|The rate of events processed by the Arrow sink.|#events/second||
|`ascii-writer.rate`|The rate of events processed by the ascii sink.|#events/second||
|`csv-reader.rate`|The rate of events processed by the CSV source.|#events/second||
|`csv-writer.rate`|The rate of events processed by the CSV sink.|#events/second||
|`exporter.processed`|The number of processed events for the current query.|#events|🔎|
|`exporter.results`|The number of results for the current query.|#events|🔎|
|`exporter.runtime`|The runtime for the current query in nanoseconds.|nanoseconds|🔎|
|`exporter.selectivity`|The rate of results out of processed events.|#events-results/#events-processed|🔎|
|`exporter.shipped`|The number of shipped events for the current query.|#events|🔎|
|`importer.rate`|The rate of events processed by the importer component.|#events/second||
|`ingest.rate`|The ingest rate keyed by the schema name.|#events/second|🗂️|
|`json-reader.invalid-line`|The number of invalid NDJSON lines.|#events||
|`json-reader.rate`|The rate of events processed by the JSON source.|#events/second||
|`json-reader.unknown-layout`|The number if NDJSON lines with an unknown layout.|#event||
|`json-writer.rate`|The rate of events processed by the JSON sink.|#events/second||
|`catalog.lookup.candidates`|The number of candidate partitions considered for a query.|#partitions|🔎|
|`catalog.lookup.runtime`|The duration of a query evaluation in the catalog.|#nanoseconds|🔎|
|`catalog.lookup.hits`|The number of results of a query in the catalog.|#events|🔎|
|`catalog.num-partitions`|The number of partitions registered in the catalog per schema.|#partitions|🗂️#️⃣|
|`catalog.num-events`|The number of events registered in the catalog per schema.|#events|🗂️#️⃣|
|`node_throughput.rate`|The rate of events processed by the node component.|#events/second||
|`null-writer.rate`|The rate of events processed by the null sink.|#events/second||
|`partition.events-written`|The number of events written in one partition.|#events|🗂|
|`partition.lookup.runtime`|The duration of a query evaluation in one partition.|#nanoseconds|🔎💽|
|`partition.lookup.hits`|The number of results of a query in one partition.|#events|🔎💽|
|`pcap-reader.discard-rate`|The rate of packets discarded.|#events-dropped/#events-received||
|`pcap-reader.discard`|The number of packets discarded by the reader.|#events||
|`pcap-reader.drop-rate`|The rate of packets dropped.|#events-dropped/#events-received||
|`pcap-reader.drop`|The number of packets dropped by the reader.|#events||
|`pcap-reader.ifdrop`|The number of packets dropped by the network interface.|#events||
|`pcap-reader.rate`|The rate of events processed by the PCAP source.|#events/second||
|`pcap-reader.recv`|The number of packets received.|#events||
|`pcap-writer.rate`|The rate of events processed by the PCAP sink.|#events/second||
|`scheduler.backlog.custom`|The number of custom priority queries in the backlog.|#queries||
|`scheduler.backlog.low`|The number of low priority queries in the backlog.|#queries||
|`scheduler.backlog.normal`|The number of normal priority queries in the backlog.|#queries||
|`scheduler.partition.current-lookups`|The number of partition lookups that are currently running.|#workers||
|`scheduler.partition.lookups`|Query lookups executed on individual partitions.|#partition-lookups||
|`scheduler.partition.materializations`|Partitions loaded from disk.|#partitions||
|`scheduler.partition.pending`|The number of queued partitions.|#partitions||
|`scheduler.partition.remaining-capacity`|The number of partition lookups that could be scheduled immediately.|#workers||
|`scheduler.partition.scheduled`|The number of scheduled partitions.|#partitions||
|`segment-store.lookup.runtime`|The duration of a query evaluation in a partition store.|#nanoseconds|🔎💾|
|`segment-store.lookup.hits`|The number of results of a query in a partition store.|#events|🔎💾|
|`parquet-store.lookup.runtime`|The duration of a query evaluation in a parquet store.|#nanoseconds|🔎→
|`parquet-store.lookup.hits`|The number of results of a query in a parquet store.|#events|🔎💾|
|`source.start`|Timepoint when the source started.|nanoseconds since epoch||
|`source.stop`|Timepoint when the source stopped.|nanoseconds since epoch||
|`syslog-reader.rate`|The rate of events processed by the syslog source.|#events/second||
|`test-reader.rate`|The rate of events processed by the test source.|#events/second||
|`zeek-reader.rate`|The rate of events processed by the Zeek source.|#events/second||

The metadata symbols have the following meaning:

|Symbol|Key|Value|
|-:|-|-|
|🔎|`query`|A UUID to identify the query.|
|💽|`partition-type`|One of "active" or "passive".|
|#️⃣|`partition-version`|The internal partition version.|
|💾|`store-type`|One of "active" or "passive".|
|🗂️|`schema`|The schema name.|

For all keys that show throughput rates in #events/second, e.g.,
`<component>.rate`, the keys `<component>.events` and `<component>.duration` are
dividend and divisor respectively. They are not listed explicitly in the above
table.

Generally, counts are reset after a telemetry report is sent out by a component.
E.g., the total number of invalid lines the JSON reader encountered is reflected
by the sum of all `json-reader.invalid-line` events.
