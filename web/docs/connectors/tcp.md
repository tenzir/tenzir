# tcp

Loads bytes from a TCP or TLS connection.

## Synopsis

```
tcp [--listen] [-k] [--tls] [--certfile] [--keyfile] <uri>
```

## Description

The `tcp` loader establishes a TCP or TLS connection and reads bytes from it.

It can either connect to a remote endpoint, or listen on a given address and
wait for incoming connections.

### `<uri>`

The address of the remote endpoint, or the bind address if `--listen` is
specified.

### `--listen`

Ignore any predefined credentials and try to load/save with anonymous
credentials.

### `--keep-listening`

When a connection is closed, wait for another incoming connection instead of
closing the pipeline. Requires `--listen`.

### `--tls`

Wrap the connection into a TLS secured stream.

### `--certfile`

Path to a `.pem` file containing the TLS certificate for the server.
Ignored unless `--tls` is also specified.

### `--keyfile`

Path to a `.pem` file containing the private key for the certificate.
Ignored unless `--tls` is also specified.

## Examples

### Connect

Read tcp data from a plain TCP endpoint:
```
load tcp://127.0.0.1:8000
```

To test this locally, one could spin up a local server like this:

```
echo foo | nc -N -l 127.0.0.1 8000
```

### Listen

Listen on localhost and wait for incoming TLS connections:
```
load tcp://127.0.0.1:4000 --listen --certfile ./key_and_cert.pem --keyfile ./key_and_cert.pem -k
```

This example uses a self-signed certificate that can be generated like this:
```
openssl req -x509 -newkey rsa:2048 -keyout key_and_cert.pem -out key_and_cert.pem -days 365 -nodes
```

The endpoint can be tested locally using a command like:
```
openssl s_client 127.0.0.1:4000
```
