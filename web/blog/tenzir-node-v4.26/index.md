---
title: "Tenzir Node v4.26: Amazon Security Lake Integration"
slug: tenzir-node-v4.26
authors: [lava]
date: 2025-01-21
tags: [release, node]
comments: true
---

# Tenzir Node v4.26: Amazon Security Lake Integration

Tenzir Node v4.26 continues to leverage our native Parquet and OCSF
capabilities by adding a new operator to write data directly to
Amazon Security Lake.

![Tenzir Node v4.26](tenzir-node-v4.26.excalidraw.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.26.0

<!-- truncate -->

## Amazon Security Lake

The new `to_asl` operator allows users to write data directly to the
Amazon Security Lake:

```tql
let $s3_uri = "s3://aws-security-data-lake-eu-west-2-lake-abcdefghijklmnopqrstuvwxyz1234/ext/tenzir_network_activity/"
 
load_kafka "ocsf_data"
read_ndjson
where @name == "ocsf.network_activity"
to_asl $s3_uri,
  region="eu-west-2",
  accountId="123456789012"
```

For more information, look at the [Integrations](/next/integrations/amazon/security-lake)
page in our documentation.

### Output Changes

With this version, we made some changes to the default output format
of the Tenzir Node in order to make it easier for downstream tooling
to work with the data provided by a Tenzir Node.

:::warning Potentially Breaking Change

This may break automations that parse the JSON output of a Tenzir Pipeline.
:::

### TQL Output

By default, the node now outputs pipeline results as TQL rather than JSON.

TQL is a superset of JSON, but it includes support for native IP, subnet,
timestamp and duration types which were formerly rendered as strings.

Additionally, TQL also supports and outputs trailing commas by default.

To restore the previous behavior, use the `write_json` sink, or set
a default sink to revert it globally:

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

Now becomes:

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

Timestamps are now printed with a `Z` suffix to indicate that they are relative
to UTC. Furthermore, the fractional part of the seconds is no longer always
printed using 6 digits. Instead, timestamps that do not have sub-second
information no longer have a fractional part. Other timestamps are either
printed with 3, 6 or 9 fractional digits, depending on their resolution.

Durations that are printed as minutes now use `min` instead of `m`.
Additionally, the fractional part of durations is now printed with full
precision instead of being rounded to two digits.

### TQL Updates

We are continously working on expanding the feature set available for writing
pipelines. This release adds two new functions, one new operator, and adds
more options to the JSON output.

#### Functions

You can use the new `string.match_regex(regex:string)` function to check whether
a string partially matches a regular expression.

The new `merge` function combines two records. `merge(foo, bar)` is a shorthand
for `{...foo, ...bar}`.

#### Operators

You can use the new `write_tql` operator to print events as TQL expressions.

We added `strip` options to `write_json` and `write_ndjson`, allowing you to
strip null fields as well as empty records or lists.

## Let's Connect!

We’re excited to engage with our community!
Join us every second Tuesday at 5 PM CET for office hours on [Discord][discord].
Share your ideas, preview upcoming features, or chat with fellow Tenzir users
and our team. Bring your questions, use cases, or just stop by to say hello!

[discord]: /discord  
[changelog]: /changelog#v4260
