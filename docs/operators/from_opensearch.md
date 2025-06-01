---
title: from_opensearch
category: Inputs/Events
example: 'from_opensearch'
---

Receives events via Opensearch Bulk API.

```tql
from_opensearch [url:string, keep_actions=bool, max_request_size=int, tls=bool,
                 certfile=string, keyfile=string, password=string]
```

## Description

The `from_opensearch` operator emulates simple situations for the [Opensearch
Bulk
API](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/).

### `url: string (optional)`

URL to listen on.

Must have the form `host[:port]`.

Defaults to `"0.0.0.0:9200"`.

### `keep_actions = bool (optional)`

Whether to keep the command objects such as `{"create": ...}`.

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
from_opensearch "1.2.3.4:8080"
```

### Listen with TLS

```tql
from_opensearch tls=true, certfile="server.crt", keyfile="private.key"
```

## See also

[`to_opensearch`](/reference/operators/to_opensearch)
