# from_opensearch

Receives events via [Opensearch Bulk API](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/).

## Synopsis

```tql
from_opensearch [port=int, address=string, keep_actions=bool,
                max_request_size=int, tls=bool, certfile=string,
                keyfile=string, password=string]
```

## Description

The `from_opensearch` operator emulates simple situations for the Opensearch
Bulk API.

### `port = int (optional)`

Port to listen on.

Defaults to `9200`.

### `address = string (optional)`

The address to listen on.

Defaults to `"0.0.0.0"`.

### `keep_actions = bool (optional)`

If to keep the command objects such as `{"create": ...}`.

Defaults to `false`.

### `max_request_size = int (optional)`

The maximum size of an incoming request to accept.

Defaults to `10Mib`.

### `tls = bool (optional)`

Enables TLS.

Defaults to `false`.

### `certfile = string (optional)`

Path to the client certificate. Required if `tls` is `true`.

### `keyfile = string (optional)`

Path to the key for the client certificate. Required if `tls` is `true`.

### `password = string (optional)`

Password for keyfile.

## Examples

### Listen on port 8080 on an interface with IP 1.2.3.4

```tql
from_opensearch port=8080, address="1.2.3.4"
```

### Listen with TLS

```tql
from_opensearch tls=true, certfile="server.crt", keyfile="private.key"
```
