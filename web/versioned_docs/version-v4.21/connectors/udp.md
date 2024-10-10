---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# udp

Loads bytes from and saves bytes to a UDP socket.

## Synopsis

Loader:

```
udp [-c|--connect] [-n|--insert-newlines] <endpoint>
```

Saver:

```
udp <endpoint>
```

## Description

The `udp` connector supports UDP sockets. The loader reads blocks of
bytes from the socket, and the saver writes them to the socket.

The loader defaults to creating a socket in listening mode. Use `--connect` if
the loader should initiate the connection instead.

When you have a socket in listening mode, use `0.0.0.0` to accept connections on
all interfaces. The [`nics`](../operators/nics.md) operator lists all all
available interfaces.

### `<endpoint>`

The address of the remote endpoint to connect to when using `--connect`, and the
bind address when using `--listen`.

### `-c,--connect` (Loader)

Connect to `<endpoint>` instead of listening at it.

### `-n,--insert-newlines` (Saver, Loader)

Append a newline character (`\n`) at the end of every datagram.

This option comes in handy in combination with line-based parsers downstream,
such as NDJSON.

## Examples

Import JSON via UDP by listenting on IP address `127.0.0.1` at port `56789`:

```
from udp://127.0.0.1:56789
| import
```

Use a shell to test the UDP loader with netcat:

```bash title="Shell 1"
tenzir 'from udp://127.0.0.1:56789'
```

```bash title="Shell 2"
jq -n '{foo: 42}' | nc -u 127.0.0.1 56789
```

Send the Tenzir version as CSV file to a remote endpoint via UDP:

```
version
| write csv
| save udp 127.0.0.1:56789
```

Use `nc -ul 127.0.0.1 56789` to spin up a UDP server to test the above pipeline.
