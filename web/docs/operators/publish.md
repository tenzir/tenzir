---
sidebar_custom_props:
  operator:
    sink: true
---

# publish

Publishes events at a Tenzir Node. The dual to [`subscribe`](subscribe.md).

## Synopsis

```
publish [<topic>]
```
## Description

The `publish` operator publishes events at a Tenzir Node in a channel with the
specified topic. Any number of subscribers using the [`subscribe`](subscribe.md)
operator receive the events immediately.

### `<topic>`

An optional topic for publishing events under. The provided topic must be
unique.

## Examples

Publish Zeek conn logs at a Tenzir Node under the topic `zeek-conn`.

```
from file conn.log read zeek-tsv | publish zeek-conn
```
