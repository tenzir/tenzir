# ZeroMQ

[ZeroMQ](https://zeromq.org/) (0mq) is a light-weight messaging framework with
various socket types. Tenzir supports writing to [PUB
sockets](https://zeromq.org/socket-api/#pub-socket) and reading from [SUB
sockets](https://zeromq.org/socket-api/#sub-socket), both in server (listening)
and client (connect) mode.

![ZeroMQ](zeromq.svg)

Use the IP address `0.0.0.0` to listen on all available network interfaces.

Because ZeroMQ is entirely asynchronous, publishers send messages even when no
subscriber is present. This can lead to lost messages when the publisher begins
operating before the subscriber. To avoid data loss due to such races, pass
`monitor=true` to activate message buffering until at least one remote peer has
connected.

:::tip URL Support
The URL scheme `zmq://` dispatches to
[`load_zmq`](../../tql2/operators/load_zmq.md) and
[`save_zmq`](../../tql2/operators/save_zmq.md) for seamless URL-style use via
[`from`](../../tql2/operators/from.md) and [`to`](../../tql2/operators/to.md).
:::

## Examples

### Accept Syslog messages over UDP

```tql
from "zmq://127.0.0.1:541" {
  read_syslog
}
```

### Send events to a UDP socket

```tql
from {message: "Tenzir"}
to "zmq://1.2.3.4:8080" {
  write_ndjson
}
```
