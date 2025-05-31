---
title: publish
category: Connecting Pipelines
example: 'publish "topic"'
---
Publishes events to a channel with a topic.

```tql
publish [topic:string]
```
## Description

The `publish` operator publishes events at a node in a channel with the
specified topic. All [`subscribers`](/reference/operators/subscribe) of the channel operator
receive the events immediately.

:::note
The `publish` operator does not guarantee that events stay in their
original order.
:::

### `topic: string (optional)`

An optional topic for publishing events under. If unspecified, the operator
publishes events to the topic `main`.

## Examples

### Publish Zeek connection logs under the topic `zeek`

```tql
load_file "conn.log"
read_zeek_tsv
publish "zeek"
```

## See Also

[`import`](/reference/operators/import),
[`subscribe`](/reference/operators/subscribe)
