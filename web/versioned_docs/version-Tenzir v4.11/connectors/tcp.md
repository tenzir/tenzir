---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# tcp

Loads bytes from a TCP or TLS connection.

## Synopsis

Loader:

```
tcp [-c|--connect] [-o|--listen-once]
    [--tls] [--certfile] [--keyfile] <endpoint>
```

Saver:

```
tcp [-l|--listen] [-o|--listen-once]
    [--tls] [--certfile] [--keyfile] <endpoint>
```

## Description

The `tcp` connector supports TCP or TLS connections. The loader reads blocks of
bytes from the socket, and the saver writes them to the socket.

The loader defaults to creating a socket in listening mode. Use `--connect` if
the loader should initiate the connection instead. The saver defaults to
creating a socket in connect mode. Use `--listen` if the saver should instead
listen on a socket.

When you have a socket in listening mode, use `0.0.0.0` to accept connections on
all interfaces. Both saver and loader also have a `--listen-once` option that
will stop the pipeline after the first connection terminated. The
[`nics`](../operators/nics.md) operator lists all all available interfaces.

:::caution One connection at at time
A single pipeline can accept at most *one* TCP connection at a time. If another
client attempts to connect to the same listening socket, it will time out. The
reason for this behavior is that the downstream operator (typically a parser)
may exhibit undefined behavior if it receives data from multiple sockets.
:::

### `<endpoint>`

The address of the remote endpoint to connect to when using `--connect`, and the
bind address when using `--listen`.

### `-c,--connect` (Loader)

Connect to `<endpoint>` instead of listening at it.

### `-l,--listen` (Saver)

Listen at `<endpoint>` instead of connecting to it.

### `-o,--listen-once`

When listening to a socket, only process a single connection instead of looping
over all connecting clients forever.

Requires a loader or saver with `--listen`.

### `--tls`

Wrap the connection into a TLS secured stream.

### `--certfile`

Path to a `.pem` file containing the TLS certificate for the server.

Ignored unless `--tls` is also specified.

### `--keyfile`

Path to a `.pem` file containing the private key for the certificate.

Ignored unless `--tls` is also specified.

## Examples

Read raw bytes by connecting to a TCP endpoint:

```
load tcp://127.0.0.1:8000
```

Test this locally by spinning up a local server with `socat`:

```bash
echo foo | socat TCP-LISTEN:8000 stdout
```

Listen on localhost and wait for incoming TLS connections:

```
load tcp://127.0.0.1:4000 --tls --certfile key_and_cert.pem --keyfile key_and_cert.pem -k
```

The above example uses a self-signed certificate that can be generated like this:

```bash
openssl req -x509 -newkey rsa:2048 -keyout key_and_cert.pem -out key_and_cert.pem -days 365 -nodes
```

Test the endpoint locally by issuing a TLS connection:

```bash
openssl s_client 127.0.0.1:4000
```
