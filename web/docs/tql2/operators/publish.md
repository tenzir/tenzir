# publish

Publishes events to a channel with a topic. The dual to
[`subscribe`](subscribe.md).

```
publish [topic=str]
```
## Description

The `publish` operator publishes events at a node in a channel with the
specified topic. All [`subscribers`](subscribe.md) of the channel operator receive the 
events immediately.

:::note
The `publish` operator does not guarantee that events stay in their
original order.
:::

### `topic`

An optional topic for publishing events under.

## Examples

XXX: Fix example

Publish Zeek conn logs under the topic `"zeek-conn"`.

```
from file conn.log read zeek-tsv | publish "zeek-conn"
```
