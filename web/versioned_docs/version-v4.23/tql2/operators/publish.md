# publish

Publishes events to a channel with a topic. The dual to
[`subscribe`](subscribe.md).

```tql
publish [topic:str]
```
## Description

The `publish` operator publishes events at a node in a channel with the
specified topic. All [`subscribers`](subscribe.md) of the channel operator
receive the events immediately.

:::note
The `publish` operator does not guarantee that events stay in their
original order.
:::

### `topic: str (optional)`

An optional topic for publishing events under. If unspecified, the operator
publishes events to a global unnamed feed.

## Examples

### Publish Zeek connection logs under the topic `zeek`

```tql
load_file "conn.log"
read_zeek_tsv
publish "zeek"
```
