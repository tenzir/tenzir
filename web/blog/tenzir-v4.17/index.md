---
title: Tenzir v4.17
authors: [dominiklohmann]
date: 2024-06-21
tags: [release, azure-log-analytics, lookup-table]
comments: true
---

The new [Tenzir v4.17][github-release] brings an integration with Azure Log
Analytics and adds support for expiring entries in lookup tables.

![Tenzir v4.17](tenzir-v4.17.excalidraw.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.17.0

<!-- truncate -->

## Send Events to Azure Log Analytics

The shining star of Tenzir v4.17 is the new [`azure-log-analytics` sink
operator][azure-log-analytics-operator], which sends events to [Log Analytics in
Azure Monitor][log-analytics-overview].

:::tip Want to Learn More?
We wrote an [integration guide][azure-log-analytics-integration] showing how to
send your events to Azure Log Analytics using Tenzir. Come check it out!
:::

[azure-log-analytics-operator]: /next/operators/azure-log-analytics
[log-analytics-overview]: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/log-analytics-overview
[azure-log-analytics-integration]: /next/integrations/azure-log-analytics

## Lookup Table Timeouts

The `context update` operator gained two new options when used together with
[lookup table contexts][lookup-table-docs]: `--create-timeout <duration>` and
`--update-timeout <duration>`.

Both new options cause individual events to expire in the lookup table. Create
timeouts specify the time after which entries in the lookup table expire, and
update timeouts specify the time after which entries in the lookup table expire
when they're not accessed.

The following example adds lookup table entries that expire after a week at the
latest, or when they were not accessed for a day, whichever comes first:

```
…
| context update my-lookup-table --create-timeout 1w --update-timeout 1d
```

[lookup-table-docs]: /next/contexts/lookup-table

## Print Individual Fields in Events

The [`print <field> <format>` operator][print-operator-docs] is the counterpart
to the [`parse` operator][parse-operator-docs]. Given a field of type record
within an event, it replaces it with a string containing the formatted
representation. This is best explained on an example:

```json {0} title="Input"
{
  "flow_id": 852833247340038,
  "flow": {
    "pkts_toserver": 1,
    "pkts_toclient": 0,
    "bytes_toserver": 54,
    "bytes_toclient": 0
  }
}
```

```text {0} title="Render the field flow as CSV"
from input.json
| print flow csv --no-header
```

```json {0} title="Output"
{
  "flow_id": 852833247340038,
  "flow": "1,0,54,0"
}
```

The `print` operator is especially useful when working with third-party APIs
that often do not support deeply nested data structures in their data model.

[print-operator-docs]: /next/operators/print
[parse-operator-docs]: /next/operators/parse

## Changes to Built-in Type Aliases

We removed the built-in `timestamp` and `port` type aliases for `time` and
`uint64`, respectively.

These types were relics of Tenzir's past, when onboarding data required
specifying a schema explicitly. Back then, we started using type aliases to
further categorize parts of the onboarded data. With Tenzir today, automatic
schema inference is the modus operandi. This caused data that was imported with
a schema to sometimes use a `timestamp` type, but all automatically inferred
data used the underlying `time` type. This caused issues down the line, because
operators like `summarize` by design do not group fields together with distinct
types. To users, this showed as duplicate values that were supposed to be
grouped by in summarized results.

:::warning Required Configuration Changes
If you have custom schemas installed in `/opt/tenzir/etc/tenzir/schemas` or
`~/.config/tenzir/schemas`, you will need to adapt them in one of two ways:
1. Replace all `timestamp` types with `time` and all `port` types with `uint64`
   (recommended).
2. Add the aliases back to your own schemas by defining `type timestamp = time`
   and `type port = uint64`, respectively.
:::

## Edit Pipelines in the Tenzir Platform

You can now change pipelines on [app.tenzir.com][tenzir-app] more quickly.
Simply click on any pipeline on the overview page to open a detailed view. In
this view, you can directly edit the definition or options. The new action menu
allows you to quickly start, pause, stop, duplicate, or delete a pipeline.

## Other Changes

For a full list of changes in this release, please check our
[changelog][changelog], and play with the new changes at
[app.tenzir.com][tenzir-app].

Every second Tuesday at 8 AM EST / 11 AM EST / 5 PM CET / 9.30 PM IST, we hold
office hours in [our Discord server][discord]. Join us next week for an
exclusive sneak peek with our designer into upcoming changes to
[app.tenzir.com][tenzir-app]!

[discord]: /discord
[changelog]: /changelog#v4170
[tenzir-app]: https://app.tenzir.com
