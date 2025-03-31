---
title: "Tenzir Node v4.31: OpenSearch Ingestion and if-expressions"
slug: tenzir-node-v4.31
authors: [IyeOnline, raxyte]
date: 2025-03-31
tags: [release, node]
comments: true
---

[Tenzir Node v4.31][github-release] is now available, with several new features
including OpenSearch emulation for broader data ingestion, flexible if/else
expressions in TQL, and a new operator for writing Syslog messages.

![Tenzir Node v4.31](tenzir-node-v4.31.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.31.0

<!-- truncate -->

## Emulating OpenSearch

Tenzir can now receive data from a variety of tools such as
[Beats](https://www.elastic.co/beats) using their OpenSearch/Elasticsearch
outputs via the new [`from_opensearch`](/next/tql2/operators/from_opensearch)
operator.

For example, to receive events from [Filebeat](https://www.elastic.co/beats/filebeat):

Setup an Elasticsearch sink for Filebeat:

```yml title="filebeat.yml"
output.elasticsearch:
  hosts: ["<host to forward to>"]
```

and listen on the host with:

```tql
from_opensearch keep_actions=true
```

```tql
{create:{_index:"filebeat-8.17.3"}}
{"@timestamp":2025-03-31T13:42:28.068Z,log:{offset:1,file:{path:"/mounted/logfile"}},message:"hello",input:{type:"log"},host:{name:"eb21"},agent:{id:"682cfcf4-f251-4576-abcb-6c8bcadfda08",name:"eb21",type:"filebeat",version:"8.17.3",ephemeral_id:"17f74f6e-36f0-4045-93e6-c549874716df"},ecs:{version:"8.0.0"}}
{create:{_index:"filebeat-8.17.3"}}
{"@timestamp":2025-03-31T13:42:28.068Z,log:{offset:7,file:{path:"/mounted/logfile"}},message:"this",input:{type:"log"},host:{name:"eb21"},agent:{id:"682cfcf4-f251-4576-abcb-6c8bcadfda08",name:"eb21",type:"filebeat",version:"8.17.3",ephemeral_id:"17f74f6e-36f0-4045-93e6-c549874716df"},ecs:{version:"8.0.0"}}
```

## `if … else …` expressions

This release also brings `if` and `else` expressions. They work similar to the
short-hand form in Python.

The rules are very simple:
- `value if true` => `value`
- `value if false` => `null`
- `value if null` => `null` and warning
- `value else fallback` => `value`
- `null else fallback` => `null`

These can be combined:
- `value if true else fallback` => `value`
- `value if false else fallback` => `fallback`
- `value if null else fallback` => `fallback` and warning

For example, `if … else …` combines nicely with the new `metrics "pipeline"`:

```tql
metrics "pipeline"
summarize (
  pipeline_id,
  ingress=sum(ingress.bytes / ingress.duration.count_seconds() if not
ingress.internal else 0.0),
  from_node=sum(ingress.bytes / ingress.duration.count_seconds() if
ingress.internal else 0.0),
  egress=sum(egress.bytes / egress.duration.count_seconds() if not
egress.internal else 0.0),
  to_node=sum(egress.bytes / egress.duration.count_seconds() if egress.internal
else 0.0),
)
```

This returns an overview of the ingress and egress of all pipelines, but
additionally separates out node-internal ingress and egress. The latter was not
easily possible before.

:::tip Standalone `else`
Note that `else` can also be used standalone, and is effectively a superior
version of the `otherwise` function. That is, `foo.otherwise(bar)` is now better
written as `foo else bar`.
:::

## Writing Syslog Messages

With this release, we also introduce the `write_syslog` operator that converts
incoming events into [RFC 5424](https://datatracker.ietf.org/doc/html/rfc5424)
Syslog Messages.

```tql title="Example pipeline"
from {
  facility: 1,
  severity: 1,
  timestamp: now(),
  hostname: "localhost",
  structured_data: {
    origin: {
      key: "value",
    },
  },
  message: "Tenzir",
}
write_syslog
```

```log
<9>1 2025-03-31T13:28:55.971210Z localhost - - - [origin key="value"] Tenzir
```

## Fixes, Improvements & Other Small Changes

This release also contains a number of small fixes and improvements, which you
can find in the [changelog][changelog].

## Let's Connect

Do you want to directly engage with Tenzir? Join our [Discord server][discord],
where we discuss projects and features and host our bi-weekly office hours
(every second Tuesday at 5 PM CET). Regardless of whether you just want to hang
out or have that one very specific question you just need answered, you are always
welcome!

[discord]: /discord
[changelog]: /changelog#v4310
