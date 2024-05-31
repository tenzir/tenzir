---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# zmq

Loads bytes from and saves bytes to ZeroMQ messages.

## Synopsis

```
zmq [-l|--listen] [-c|--connect] [-m|--monitor] [<endpoint>]
```

## Description

The `zmq` loader processes the bytes in a ZeroMQ message received by a `SUB`
socket. The `zmq` saver sends bytes as a ZeroMQ message via a `PUB` socket.

![ZeroMQ Connector](zeromq-connector.excalidraw.svg)

Indpendent of the socket type, the `zmq` connector supports specfiying the
direction of connection establishment with `--listen` and `--connect`. This can be
helpful to work around firewall restrictions and fit into broader set of
existing ZeroMQ applications.

With the `--monitor` option, you can activate message buffering for TCP
sockets that hold off sending messages until *at least one* remote peer has
connected. This can be helpful when you want to delay publishing until you have
one connected subscriber, e.g., when the publisher spawns before any subscriber
exists.

The default format for the `zmq` connector is [`json`](../formats/json.md).

### `-f|--filter <prefix>` (Loader)

Installs a filter for the ZeroMQ `SUB` socket at the source. Filting in ZeroMQ
means performing a prefix-match on the raw bytes of the entire message.

Defaults to the empty string, which is equivalent to no filtering.

### `-l|--listen`

Bind to the ZeroMQ socket.

By default, the loader connects and the saver listens.

### `-c|--connect`

Connect to the ZeroMQ socket.

By default, the loader connects and the saver listens.

### `-m|--monitor`

Monitors a 0mq socket over TCP until the remote side establishes a connection.

### `<endpoint>`

The endpoint for connecting to or listening on a ZeroMQ socket.

Defaults to `tcp://127.0.0.1:5555`.

## Examples

Publish query results to a ZeroMQ socket:

```
export | where x == 42 | to zmq
```

Publish the list of TQL operators as [CSV](../formats/csv.md), also connect
instead of listening on the ZeroMQ socket:

```
show operators | to zmq -c write csv
```

Interpret ZeroMQ messages as [JSON](../formats/json.md):

```
from zmq 1.2.3.4:56789 read json
```

You could drop `read json` above since `json` is the default format for `zmq`.
