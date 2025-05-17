# http

Sends HTTP/1.1 requests.

## Synopsis

```tql
http url:string, [method=string, payload=string, headers=record, tls=bool,
    certfile=string, keyfile=string, password=string,
    insert_metadata_into=field, connection_timeout=duration,
    max_retry_count=int, retry_delay=duration, max_inflight_requests=int,
    parse={ … }]
```

## Description

The `http` operator issues HTTP/1.1 requests and forwards received responses as events.

### `url: string`

URL to connect to.

### `method = string (optional)`

One of the following HTTP method to use when using the client:
- `get`
- `head`
- `post`
- `put`
- `del`
- `connect`
- `options`
- `trace`

Defaults to `get`, or `post` if `payload` is specified.

### `payload = string (optional)`

Payload to send with the HTTP request.

### `headers = record (optional)`

Record of headers to send with the request.

### `tls = bool (optional)`

Enables TLS.

Defaults to `false`.

### `certfile = string (optional)`

Path to the client certificate. Required if `tls` is `true`.

### `keyfile = string (optional)`

Path to the key for the client certificate. Required if `tls` is `true`.

### `password = string (optional)`

Password for keyfile.

### `connection_timeout = duration (optional)`

Timeout for the connection.

Defaults to `5s`.

### `max_retry_count = int (optional)`

The maximum times to retry a failed request.

Defaults to `0`.

### `retry_delay = duration (optional)`

Duration to wait between each retry.

Defaults to `1s`.

### `max_inflight_requests = int (optional)`

Maximum amount of requests that can be in progress at any time.

Defaults to `1`.

### `insert_metadata_into = field (optional)`

Field to insert metadata into when using the `parse` pipeline.

### `parse = { … } (optional)`

A pipeline that receives the response body as bytes, allowing parsing per
request.

## Examples

### Make a GET request

```tql
from {url: "https://tenzir.com"}
http url
```

```tql
{
  code: 308,
  headers: {
    "Alt-Svc": "h3=\":443\"; ma=2592000",
    Date: "Mon, 12 May 2025 12:33:26 GMT",
    Location: "https://www.tenzir.com/",
    Server: "Framer/fab5c4d",
    "Strict-Transport-Security": "max-age=31536000",
    "X-Content-Type-Options": "nosniff",
    "Content-Length": "0",
  },
}
```

###

```tql
from {}
```
