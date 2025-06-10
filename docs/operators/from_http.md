---
title: from_http
category: Inputs/Events
example: 'from_http "0.0.0.0:8080'
---

Sends and receives HTTP/1.1 requests.

```tql
from_http url:string, [method=string, payload=string, headers=record,
          metadata_field=field, paginate=record->string,
          paginate_delay=duration, connection_timeout=duration,
          max_retry_count=int, retry_delay=duration, responses=record,
          max_request_size=int, tls=bool, certfile=string, keyfile=string,
          password=string { … }]
from_http url:string, server=true, [responses=record, max_request_size=int,
          tls=bool, certfile=string, keyfile=string, password=string { … }]
```

## Description

The `from_http` operator issues HTTP requests or spins up an HTTP/1.1 server on
a given address and forwards received requests as events.

### `url: string`

URL to listen on or to connect to.

Must have the form `<host>:<port>` when `server=true`.

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

### `metadata_field = field (optional)`

Field to insert metadata into when using the parsing pipeline.

The metadata has the following schema:

| Field                | Type     | Description                           |
| :------------------- | :------- | :------------------------------------ |
| `code`               | `uint64` | The HTTP status code of the response. |
| `headers`            | `record` | The response headers.                 |
| `query`              | `record` | The query parameters of the request.  |
| `path`               | `string` | The path requested.                   |
| `fragment`           | `string` | The URI fragment of the request.      |
| `method`             | `string` | The HTTP method of the request.       |
| `version`            | `string` | The HTTP version of the request.      |

### `paginate = record -> string (optional)`

A lambda expression to evaluate against the result of the request (optionally parsed
by the given pipeline). If the expression evaluation is successful and non-null, the
resulting string is used as the URL for a new GET request with the same headers.

### `paginate_delay = duration (optional)`

The duration to wait between consecutive paginatation requests.

Defaults to `0s`.

### `connection_timeout = duration (optional)`

Timeout for the connection.

Defaults to `5s`.

### `max_retry_count = int (optional)`

The maximum times to retry a failed request. Every request has its own retry
count.

Defaults to `0`.

### `retry_delay = duration (optional)`

The duration to wait between each retry.

Defaults to `1s`.

### `server = bool (optional)`

Whether to spin up an HTTP server or act as an HTTP client.

Defaults to `false`, i.e., the HTTP client.

### `responses = record (optional)`

Specify custom responses for endpoints on the server. For example,

```tql
responses = {
  "/resource/create": { code: 200, content_type: "text/html", body: "Created!" },
  "/resource/delete": { code: 401, content_type: "text/html", body: "Unauthorized!" }
}
```

creates two special routes on the server with different responses.

Requests to an unspecified endpoint are responded with HTTP Status `200 OK`.

### `max_request_size = int (optional)`

The maximum size of an incoming request to accept.

Defaults to `10Mib`.

### `tls = bool (optional)`

Enables TLS.

Defaults to `false`.

### `certfile = string (optional)`

Path to the client certificate. Required for server if `tls` is `true`.

### `keyfile = string (optional)`

Path to the key for the client certificate. Required for server if `tls` is `true`.

### `password = string (optional)`

Password for keyfile.

### `{ … } (optional)`

A pipeline that receives the response body as bytes, allowing parsing per
request. This is especially useful in scenarios where the response body can be
parsed into multiple events.

If not provided, the operator will attempt to infer the parsing operator from
the `Content-Type` header. Should this inference fail (e.g., unsupported or
missing `Content-Type`), the operator raises an error.

## Examples

### Make a GET request

Make a request to [urlscan.io](https://urlscan.io/docs/api#search) to search for
scans for `tenzir.com` and get the first result.

```tql
from_http "https://urlscan.io/api/v1/search?q=tenzir.com"
unroll results
head 1
```

```tql
{
  results: {
    submitter: { ... },
    task: { ... },
    stats: { ... },
    page: { ... },
    _id: "0196edb1-521e-761f-9d62-1ca4cfad5b30",
    _score: null,
    sort: [ "1747744570133", "\"0196edb1-521e-761f-9d62-1ca4cfad5b30\"" ],
    result: "https://urlscan.io/api/v1/result/0196edb1-521e-761f-9d62-1ca4cfad5b30/",
    screenshot: "https://urlscan.io/screenshots/0196edb1-521e-761f-9d62-1ca4cfad5b30.png",
  },
  total: 9,
  took: 296,
  has_more: false,
}
```

### Paginated API Requests

Use the `paginate` parameter to handle paginated APIs:

```tql
from_http "https://api.example.com/data", paginate=(x => x.next_url?)
```

This sends a GET request to the initial URL and evaluates the `x.next_url` field
in the response to determine the next URL for subsequent requests.

### Retry Failed Requests

Configure retries for failed requests:

```tql
from_http "https://api.example.com/data", max_retry_count=3, retry_delay=2s
```

This tries up to 3 times, waiting 2 seconds between each retry.

### Listen on port 8080

Spin up a server with:

```tql
from_http "0.0.0.0:8080", server=true, metadata_field=metadata
```

Send a request to the HTTP endpoint via `curl`:

```sh
echo '{"key": "value"}' | gzip | curl localhost:8080 --data-binary @- -H 'Content-Encoding: gzip' -H 'Content-Type: application/json'
```

Observe the request in the Tenzir pipeline, parsed and decompressed:

```tql
{
  key: "value",
  metadata: {
    headers: {
      Host: "localhost:8080",
      "User-Agent": "curl/8.13.0",
      Accept: "*/*",
      "Content-Encoding": "gzip",
      "Content-Length": "37",
      "Content-Type": "application/json",
    },
    path: "/",
    method: "post",
    version: "HTTP/1.1",
  },
}
```

## See Also

[`http`](/reference/operators/http),
[`serve`](/reference/operators/serve)
