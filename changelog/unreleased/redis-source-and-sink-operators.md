---
title: Redis source and sink operators
type: feature
authors:
  - mavam
created: 2026-11-09T10:58:08.527539Z
---


The new `from_redis` and `to_redis` operators read events from and write events
to Redis. Pipelines can now consume and fill work queues in Redis lists,
subscribe and publish to pub/sub channels, and keep state in Redis keys.

The `from_redis` operator pops items from one or more lists with a blocking pop
and emits one event per item, with the item in the `message` field. By default,
it pops from the right end, which consumes a list in FIFO order when producers
push to the left end:

```tql
from_redis "events", url="redis://redis.example.org:6379"
this = string(message).parse_json()
```

With `command="subscribe"` or `command="psubscribe"`, the operator receives
messages from pub/sub channels instead. The `metadata_field` option adds the
list key, channel, and pattern to every event.

The `to_redis` operator writes one value per event. It pushes to the left end
of a list by default and supports `rpush`, `publish`, `set`, `sadd`, `zadd`, and
`hset` as well, with an optional `ttl` for the written keys:

```tql
to_redis "status:" + host, command="set", message=status, ttl=1h
```

Both operators accept a `redis://` or `rediss://` URL, a database index, a user
and password as secrets, and TLS settings. They reconnect with a warning when
the connection drops. When the pipeline stops, `from_redis` emits the items of
a pop in flight, and a cancelled pipeline hands the items that it popped but did
not emit back to their list.
