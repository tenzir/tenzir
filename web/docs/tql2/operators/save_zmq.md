# save_zmq

Saves bytes to ZeroMQ messages.

```tql
save_zmq [endpoint:str, listen=bool, connect=bool, monitor=bool]
```

## Description

The `save_zmq` operator sends bytes as a ZeroMQ message via a `PUB` socket.

Indpendent of the socket type, the `zmq` connector supports specfiying the
direction of connection establishment with `listen` and `connect`. This can be
helpful to work around firewall restrictions and fit into broader set of
existing ZeroMQ applications.

With the `monitor` option, you can activate message buffering for TCP
sockets that hold off sending messages until *at least one* remote peer has
connected. This can be helpful when you want to delay publishing until you have
one connected subscriber, e.g., when the publisher spawns before any subscriber
exists.

### `endpoint: str (optional)`

The endpoint for connecting to or listening on a ZeroMQ socket.

Defaults to `tcp://127.0.0.1:5555`.

### `listen = bool (optional)`

Bind to the ZeroMQ socket.

By default, the loader connects and the saver listens.

### `connect = bool (optional)`

Connect to the ZeroMQ socket.

By default, the loader connects and the saver listens.

### `monitor = bool (optional)`

Monitors a 0mq socket over TCP until the remote side establishes a connection.

## Examples

Publish the list of plugins as [CSV](write_csv.md), also connect
instead of listening on the ZeroMQ socket:

```tql
plugins
write_csv
save_zmq connect=true
```
