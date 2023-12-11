# tcp

Loads bytes from a TCP or TLS connection.

## Synopsis

Loader:

```
tcp [-c|--connect] [-o|--once]
    [--tls] [--certfile] [--keyfile] <endpoint>
```

Saver:

```
tcp [-l|--listen] [-o|--once]
    [--tls] [--certfile] [--keyfile] <endpoint>
```

## Description

The `tcp` loader establishes a TCP or TLS connection and reads bytes from it.

It can either connect to a remote endpoint, or listen on a given address and
wait for incoming connections.

### `<endpoint>`

The address of the remote endpoint to connect to when using `--connect`, and the
bind address when using `--listen`.

### `-c,--connect` (Loader)

Connect to `<endpoint>` instead of listen at it.

### `-l,--listen` (Saver)

Listen at `<endpoint>` instead of connect to it.

### `-o,--once`

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
