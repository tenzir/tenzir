---
title: Tenzir v4.9
authors: [dominiklohmann]
date: 2024-02-21
tags: [context, bloom-filter, chart, dashboard]
comments: true
---

We're thrilled to announce the release of [Tenzir
v4.9](https://github.com/tenzir/tenzir/releases/tag/v4.9.0), enhancing the
Explorer further to empower you with the capability of rendering your data as a
chart.

![Tenzir v4.9](tenzir-v4.9.excalidraw.svg)

<!-- truncate -->

## Chart Operator

The new [`chart`](/next/operators/chart) operator transforms the way you
visualize your data on [app.tenzir.com](https://app.tenzir.com). It lets you
depict your events graphically instead of in table form.

Charting integrates seamlessly into your pipelines by simply adding the `chart`
operator. For instance, plotting a bar chart representing the frequency of
occurences for each protocol in `suricata.flow` events can be as simple as this:

TODO: Add screenshots for all charts

```
export
| where #schema == "suricata.flow"
| top proto
| chart bar --title "Protocols"
```

This line chart depicts the load average over 15 minutes for the past two days,
making use of the recently added `metrics` operator:

```
metrics
| where #schema == "tenzir.metrics.cpu"
| where #import_time > 2 days ago
| chart line -x timestamp -y loadavg_15m --title "Load Average (15 min)"
```

This area chart displays the total ingress across all pipelines for the past day
in MiB/s.

```
metrics
| where #schema == "tenzir.metrics.operator"
| where #import_time > 1 day ago
| where source == true
| where internal == false
| summarize egress=sum(output.approx_bytes), duration=sum(duration) by timestamp resolution 10 seconds
| sort timestamp
| python 'self.egress_rate = self.egress / self.duration.total_seconds() / 2**20'
| chart area -x timestamp -y egress_rate --title "Total Ingress MiB/s"
```

This pie chart shows the distribution of events stored at the node by disk
usage:

```
show partitions
| summarize diskusage=sum(diskusage) by schema
| python 'self.diskusage = self.diskusage / 2**30'
| chart pie --title "Disk Usage (GiB)"
```

We're just getting started with charting! If you want to see further chart types
added, have feedback on charting, or want to share examples of what your
visualizations with the chart operator, we would love to [hear from
you](/discord).

:::info Coming Soon: Dashboards
The `chart` operator is a first step towards having dashboards directly in
Tenzir. Any result that you see in the Explorer you will soon be able to pin and
freely arrange on customizable dashboard.
:::

## Bloom Filter Context

The new [`bloom-filter`](/next/contexts/bloom-filter) context makes it possible
to use large datasets for enrichment. It uses a [Bloom
filter](https://en.wikipedia.org/wiki/Bloom_filter) to store sets in a compact
way, at the cost of potential false positives when looking up an item.

If you have massive amounts of indicators or a large amount of things you would
liket to contextualize, this feature is for you.

Create a Bloom filter context by using `bloom-filter` as context type:

```
context create indicators bloom-filter
```

Then populate it with a pipeline, exactly like a [lookup
table](/next/contexts/lookup-table):

```
from /tmp/iocs.csv
| context update bloom-filter --key ioc
```

Thereafter use it for enrichment, e.g., in this example pipeline:

```
export --live
| where #schema == "suricata.dns"
| enrich indicators --field dns.rrname
```

The `enrich` operator gained a new `--filter` option to remove events it could
not enrich. Use the new option to remove anything that is not included in the
Bloom filter:

```
export --live
| where #schema == "suricata.dns"
| enrich indicators --field dns.rrname --filter
```

## Housekeeping

Other noteworthy changes and improvements:
- `tenzir.db-directory` is now `tenzir.state-directory`. The old option remains
  functional, but will be phased out in an upcoming release.
- On the command-line, Tenzir now respects [`NO_COLOR`](https://no-color.org)
  when printing diagnostics.
- RFC 5424-style Syslog parsing no longer omits structured data fields.
- The `--selector` option for the JSON parser now works with nested and
  non-string fields.
- The `python` operator gained a `--file` option to read from a file instead of
  expecting the Python code as a positional argument.
- The `csv`, `tsv`, and `ssv` parsers now fill in nulls for missing values.

For the curious, [the changelog](/changelog#v490) has the full scoop.

Experience the new features at [app.tenzir.com](https://app.tenzir.com) and join
us on [our Discord server](/discord).
