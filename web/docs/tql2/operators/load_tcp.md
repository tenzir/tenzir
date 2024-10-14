# load_tcp

```tql
load_tcp url:str, [connect=bool, parallel=uint, tls=bool, certfile=str, keyfile=str, { ... }]
```

## Description

Loads the contents of the `url` as a byte stream and evaluates a pipeline
for each TCP connection, if given.

### `url: str`

The `url` to load contents of. Must be of the form `[tcp://]<hostname>:<port>`.

### `connect = bool (optional)`

The HTTP Method to use.

### `parallel = uint (optional)`

Maximum number of parallel connections at once.

### `tls = bool (optional)`

Whether to use TLS.

### `certfile = str (optional)`

The TLS Certificate File to use.

### `keyfile = str (optional)`

The Key file to use.

### `{ ... } (optional)`

The pipeline to run for each individual TCP connection.

## Examples

```tql
load_tcp "tcp://example.com:10000" {
  read_syslog
}
write_json
```
