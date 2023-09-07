# zmq

Loads bytes from and saves bytes to ZeroMQ messages.

## Synopsis

```
zmq [-b|--bind] [-c|--connect] [<endpoint>]
```

## Description

The `zmq` loader processes the bytes in a ZeroMQ message received by a `SUB`
socket. The `zmq` saver sends bytes as a ZeroMQ message via a `PUB` socket.

Indpendent of the socket type, the `zmq` connector supports specfiying the
direction of connection establishment with `--bind` and `--connect`. This can be
helpful to work around firewall restrictions and fit into broader set of
existing ZeroMQ applications.

The default format for the `zmq` connector is [`json`](../formats/json.md).

### `-f|--filter <prefix>` (Loader)

Installs a filter for the ZeroMQ `SUB` socket at the source. Filting in ZeroMQ
means performing a prefix-match on the raw bytes of the entire message.

Defaults to the empty string, which is equivalent to no filtering.

### `-b|--bind`

Bind to the ZeroMQ socket.

By default, the loader connects and the saver binds.

### `-c|--connect`

Connect to the ZeroMQ socket.

By default, the loader connects and the saver binds.

### `<endpoint>`

The endpoint for connecting and binding to a ZeroMQ socket.

Defaults to `tcp://127.0.0.1:5555`.

## Examples

Publish query results to a ZeroMQ socket:

```
export | where x == 42 | to zmq
```

Publish the list of TQL operators as CSV and switch from binding to connecting:

```
show operators | to zmq -c write csv
```

Subscribe to a specific ZeroMQ endpoint:

```
from zmq 1.2.3.4:56789
```
