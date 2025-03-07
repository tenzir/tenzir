---
title: "Tenzir Node v4.26: Amazon Security Lake Integration"
slug: tenzir-node-v4.26
authors: [lava]
date: 2025-01-22
tags: [release, node]
comments: true
---

[Tenzir Node v4.26][github-release] enhances our native Parquet and OCSF capabilities with a new
operator for writing data directly to Amazon Security Lake (ASL).

![Tenzir Node v4.26](tenzir-node-v4.26.excalidraw.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.26.0

<!-- truncate -->

## Amazon Security Lake

The new `to_asl` operator enables users to write data directly to the
Amazon Security Lake:

```tql
let $s3_uri = "s3://aws-security-data-lake-eu-west-2-lake-abcdefghijklmnopqrstuvwxyz1234/ext/tenzir_network_activity/"

load_kafka "ocsf_events"
read_ndjson
where @name == "ocsf.network_activity"
to_asl $s3_uri,
  region="eu-west-2",
  accountId="123456789012"
```

For more information, visit the [Integrations](/next/integrations/amazon/security-lake)
page in our documentation.

## Output Changes

With this version, we made some changes to the default output format
of the Tenzir Node in order to make it easier for downstream tooling
to work with the data provided by a Tenzir Node.

:::warning Potentially Breaking Change
This may break automations that parse the JSON output of a Tenzir Pipeline.
:::

### TQL Output

By default, the node now outputs pipeline results as native TQL data
rather than JSON.

TQL is a superset of JSON, adding support for native IP addresses,
subnets, timestamps, and durations, which were previously represented
as strings. Additionally, TQL now includes trailing commas by default.

To restore the previous JSON output format, append the [`write_json`](/next/tql2/operators/write_json)
operator to your pipeline or configure a default sink:

```env
TENZIR_EXEC__IMPLICIT_EVENTS_SINK='write_json | save_file "-"'
```

Before:

```txt
{
  "activity_id": 16,
  "activity_name": "Query",
  "rdata": "31.3.245.133",
  "time": "2020-06-05T14:39:59.305988",
  "duration": "40s",
  "dst_endpoint": {
    "ip": "192.168.4.1",
    "port": 53
  }
}
```

Now:

```tql
{
  activity_id: 16,
  activity_name: "Query",
  rdata: 31.3.245.133,
  time: 2020-06-05T14:39:59.305988Z,
  duration: 40s,
  dst_endpoint: {
    ip: 192.168.4.1,
    port: 53,
  },
}
```

### Timestamp Rendering

Timestamps now include a `Z` suffix to indicate UTC time. The fractional seconds
display has been refined: timestamps without sub-second precision no longer
include a fractional part, while others are printed with 3, 6, or 9 decimal
places based on their resolution.

Additionally, durations expressed in minutes now use `min` instead of `m`, and
fractional durations are displayed with full precision instead of rounding to
two decimal places.

## TQL Features

We are continously working on expanding the feature set available for writing
pipelines. This release adds two new functions, one new operator, and
additional options to the JSON output.

### Functions

The new [`string.match_regex(regex:string)`](/next/tql2/functions/match_regex)
function checks whether a string partially matches a regular expression.

The new [`merge`](/next/tql2/functions/merge) function combines two records. The
expression `merge(foo, bar)` is a shorthand for `{...foo, ...bar}`.

### Operators

You can use the new [`write_tql`](/next/tql2/operators/write_tql) operator
prints events as TQL expressions.

We added `strip` options to [`write_json`](/next/tql2/operators/write_json)
and [`write_ndjson`](/next/tql2/operators/write_ndjson), allowing you to
strip null fields as well as empty records or lists.

## Let's Connect!

We’re excited to engage with our community!
Join us every second Tuesday at 5 PM CET for office hours on [Discord][discord].
Share your ideas, preview upcoming features, or chat with fellow Tenzir users
and our team. Bring your questions, use cases, or just stop by to say hello!

[discord]: /discord
[changelog]: /changelog#v4260
