---
title: Tenzir v4.18
authors: [dominiklohmann]
date: 2024-07-11
tags: [release, health-metrics, tql2]
comments: true
---

Monitoring Tenzir nodes is easier than before with [Tenzir
v4.18][github-release] and its new health metrics.

TODO: Add a title image

<!-- ![Tenzir v4.18](tenzir-v4.18.excalidraw.svg) -->

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.18

<!-- truncate -->

## Monitor Node Health With Metrics

The `metrics` operator now additionally takes a positional argument for the
metrics name. Now, `metrics cpu` is equivalent to `metrics | where #schema ==
"tenzir.metrics.cpu"`. That's a lot easier to write!

Tenzir nodes now collect more metrics than before. In particular, the `import`,
`export`, `publish`, `subscribe`, `enrich`, and `lookup` operators now emit
metrics, and nodes additionally collect `api` metrics for every API call. These
are best explained on examples:

```text {0} title="Show imported events per schema and day for the last month"
metrics import
| where timestamp > 30d ago
| summarize events=sum(events) by timestamp, schema resolution 1d
| sort timestamp, schema
```

```text {0} title="Calculate the rate of context hits for the context 'iocs'"
metrics enrich
| where context == "iocs"
| summarize events=sum(events), hits=sum(hits)
| python 'self.rate = self.hits / self.events'
```

```text {0} title="Show the most commonly used APIs in the last hour"
metrics api
| where timestamp > 1 day ago
| top path
```

These are just three examples to get you started with monitoring your node.
We're planning to make more data available from more operators in the future,
and have built a new framework that makes emitting custom metrics from operators
a breeze.

:::tip Want to learn more?
Take a look at the [`metrics` operator's documentation](/operators/metrics),
which details all the available metrics and their schema.
:::

## Play With TQL2

At this point it's an open secret that we're working working on a major revamp
to the Tenzir Query Language called TQL2. We still have quite a way to go before
making TQL2 the new default, but we're excited to announce that as of Tenzir
v4.18, it is now possible to use TQL2 without being a Tenzir developer.

To use TQL2 on [app.tenzir.com](https://app.tenzir.com), for pipelines
configured in the `tenzir.yaml` configuration file, or through the API, start
the pipeline with a `// experimental-tql2` comment. For example:

```
// experimental-tql2
source {
    foo: random(),
}
| repeat 10
| bar = random()
| baz = sqrt(foo * bar) < 0.5
```

Use `tenzir --tql2 <pipeline>` to use TQL2 with the `tenzir` binary on the
command-line.

:::warning Experimental
Many things in TQL2 are not implemented, not documented, or not yet working
correctly. We are still making breaking changes to it, so we would like to ask
you not to use it in production.

Got feedback? Head over to the `#developers` channel in our [community
Discord](/discord).
:::

## Other Changes

As usual, the [changelog][changelog] contains a full list of features, changes,
and bug fixes in this release.

Every second Tuesday at 8 AM EST / 11 AM EST / 5 PM CET / 9.30 PM IST, we hold
office hours in [our Discord server][discord]. Whether you want to participate
in the TQL2 discussion, have ideas for further metrics, feedback of any kind, a
wild idea that you'd like to bring up, or just want to hang out—come join us!

[discord]: /discord
[changelog]: /changelog#v4180
