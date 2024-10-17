# subscribe

Subscribes to events from a channel with a topic. The dual to
[`publish`](publish.md).

```tql
subscribe [topic:str]
```

## Description

The `subscribe` operator subscribes to events from a channel with the specified
topic. Multiple `subscribe` operators with the same topic receive the same
events.

Subscribers propagate back pressure to publishers. If a subscribing pipeline
fails to keep up, all publishers will slow down as well to a matching speed to
avoid data loss. This mechanism is disabled for pipelines that are not visible
on the overview page on [app.tenzir.com](https://app.tenzir.com), which drop
data rather than slow down their publishers.

### `topic: str (optional)`

An optional channel name to subscribe to. If unspecified, the operator
subscribes to the global unnamed feed.

## Examples

Subscribe to the events under the topic `zeek-conn` and filter events
([`where`](where.md)):

```tql
subscribe "zeek-conn"
where ts > now() - 1 week
```
