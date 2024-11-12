# load_udp

Loads bytes from a UDP socket.

```tql
load_udp endpoint:str, [connect=bool, insert_newlines=bool]
```

## Description

Loads bytes from a UDP socket. The operator defaults to creating a socket
in listening mode. Use `connect=true` if the operator should initiate the
connection instead.

When you have a socket in listening mode, use `0.0.0.0` to accept connections
on all interfaces. The [`nics`](./operators/nics.md) operator lists all all
available interfaces.

### `endpoint: str`

The address of the remote endpoint to load bytes from. Must be of the format:
`[udp://]host:port`.

### `connect = bool (optional)`

Connect to `endpoint` instead of listening at it.

Defaults to `false`.

### `insert_newlines = bool (optional)`

Append a newline character (`\n`) at the end of every datagram.

This option comes in handy in combination with line-based parsers downstream,
such as NDJSON.

Defaults to `false`.

## Examples

Import JSON via UDP by listenting on IP address `127.0.0.1` at port `56789`:

```tql
load_udp "127.0.0.1:56789"
import
```
