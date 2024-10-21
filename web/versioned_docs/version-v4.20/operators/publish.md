---
sidebar_custom_props:
  operator:
    sink: true
---

# publish

Publishes events to a channel with a topic. The dual to
[`subscribe`](subscribe.md).

## Synopsis

```
publish [<topic>]
```
## Description

The `publish` operator publishes events at a node in a channel with the
specified topic. Any number of subscribers using the [`subscribe`](subscribe.md)
operator receive the events immediately.

Note that the `publish` operator does not guarantee that events stay in their
original order.

### `<topic>`

An optional topic for publishing events under.

Defaults to the empty string.

## Examples

Publish Zeek conn logs under the topic `zeek-conn`.

```
from file conn.log read zeek-tsv | publish zeek-conn
```
