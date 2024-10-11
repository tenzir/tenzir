# load_tcp

```
load_tcp url:str, pipeline:{ ... }, [connect=bool, parallel=uint, tls=bool, certfile=str, keyfile=str]
```

## Description

Loads the contents of the `url` as a byte stream.

### `url:str`

The `url` to load contents of. Must be of the form `tcp://<hostname>:<port>`.

### `pipeline:{ ... }`

The `pipeline` to run for each individual TCP connection.

### `connect=bool`

The HTTP Method to use.

### `parallel=uint`

Maximum number of parallel connections at once.

### `tls=bool`

Whether to use TLS.

### `certfile=str`

The TLS Certificate File to use.

### `keyfile=str`

The Key file to use.

## Examples
