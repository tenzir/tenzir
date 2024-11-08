# load_zmq

Loads bytes from ZeroMQ messages.

```tql
load_zmq [endpoint:str, filter=str, listen=bool, connect=bool, monitor=bool]
```

## Description

The `load_zmq` operator processes the bytes in a ZeroMQ message received by a `SUB`
socket.

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

### `filter = str (optional)`

Installs a filter for the ZeroMQ `SUB` socket at the source. Filting in ZeroMQ
means performing a prefix-match on the raw bytes of the entire message.

Defaults to the empty string, which is equivalent to no filtering.

### `listen = bool (optional)`

Bind to the ZeroMQ socket.

Defaults to `false`.

### `connect = bool (optional)`

Connect to the ZeroMQ socket.

Defaults to `true`.

### `monitor = bool (optional)`

Monitors a 0mq socket over TCP until the remote side establishes a connection.

## Examples

Interpret ZeroMQ messages as [JSON](read_json.md):

```
load_zmq "1.2.3.4:56789"
read_json
```
