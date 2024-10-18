# load_tcp

Loads bytes from a TCP or TLS connection.

```tql
load_tcp endpoint:str, [connect=bool, parallel=uint, tls=bool, certfile=str, keyfile=str { … }]
```

## Description

Reads bytes from the given endpoint via TCP or TLS.

### `endpoint: str`

The endpoint at which the server will listen. If `connect=true`, this is instead
the remote endpoint to connect to. Must be of the form
`[tcp://]<hostname>:<port>`. Use the hostname `0.0.0.0` o accept connections on
all interfaces.

### `connect = bool (optional)`

Connect to the endpoint instead of listening at it.

### `parallel = uint (optional)`

Maximum number of parallel connections at once.

### `tls = bool (optional)`

Whether to use TLS.

### `certfile = str (optional)`

Path to a `.pem` file containing the TLS certificate.

### `keyfile = str (optional)`

Path to a `.pem` file containing the private key for the certificate.

### `{ … } (optional)`

The pipeline to run for each individual TCP connection. If none is specified, no
transformations are applied to the output streams. Unless you are sure that
there is at most one active connection at a time, it is recommended to specify a
pipeline that parses the individual connection streams into events, for instance
`{ read_json }`. Otherwise, the output can be interleaved.

## Examples

Listen on all network interfaces, parsing each individual connection as syslog.

```tql
load_tcp "0.0.0.0:8090" { read_syslog }
```

Connect to a remote endpoint and parse the response as JSON:

```tql
// We know that there is only one connection, so we do not specify a pipeline.
load_tcp "example.org:8090", connect=true
read_json
```

Wait for connections on localhost with TLS enabled, parsing incoming JSON
streams according to the schema `"my_schema"`, forwarding no more than 20 events
per individual connection:

```tql
load_tcp "127.0.0.1:4000", tls=true, certfile="key_and_cert.pem", keyfile="key_and_cert.pem" {
  read_json schema="my_schema"
  head 20
}
```

The example above can use a self-signed certificate that can be generated like
this:

```bash
openssl req -x509 -newkey rsa:2048 -keyout key_and_cert.pem -out key_and_cert.pem -days 365 -nodes
```

You can test the endpoint locally by issuing a TLS connection:

```bash
openssl s_client 127.0.0.1:4000
```
