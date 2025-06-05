---
title: http
category: Modify
example: 'http "example.com"'
---

Sends HTTP/1.1 requests and forwards the response.

```tql
http url:string, [method=string, payload=string, headers=record,
     response_field=field, metadata_field=field, paginate=record->string,
     paginate_delay=duration, parallel=int, tls=bool, certfile=string,
     keyfile=string, password=string, connection_timeout=duration,
     max_retry_count=int, retry_delay=duration { … }]
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
- `trace` Nice!

Defaults to `get`, or `post` if `payload` is specified.

### `payload = string (optional)`

Payload to send with the HTTP request.

### `headers = record (optional)`

Record of headers to send with the request.

### `response_field = field (optional)`

Field to insert the response into.

Defaults to `this`.

### `metadata_field = field (optional)`

Field to insert metadata into when using the parsing pipeline.

The metadata has the following schema:

| Field                | Type     | Description                           |
| :------------------- | :------- | :------------------------------------ |
| `code`               | `uint64` | The HTTP status code of the response. |
| `headers`            | `record` | The response headers.                 |

### `paginate = record -> string (optional)`

A lambda expression to evaluate against the result of the request (optionally parsed
by the given pipeline). If the expression evaluation is successful and non-null, the
resulting string is used as the URL for a new GET request with the same headers.

### `paginate_delay = duration (optional)`

The duration to wait between consecutive paginatation requests.

Defaults to `0s`.

### `parallel = int (optional)`

Maximum amount of requests that can be in progress at any time.

Defaults to `1`.

### `tls = bool (optional)`

Enables TLS.

### `certfile = string (optional)`

Path to the client certificate.

### `keyfile = string (optional)`

Path to the key for the client certificate.

### `password = string (optional)`

Password file for keyfile.

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

### `{ … } (optional)`

A pipeline that receives the response body as bytes, allowing parsing per
request. This is especially useful in scenarios where the response body can be
parsed into multiple events.

If this is not provided, the operator will attempt to infer the parsing operator
from the `Content-Type` header. Should this inference fail (e.g., unsupported or
missing `Content-Type`), a warning is raised.

## Examples

### Make a GET request

Here we make a request to [urlscan.io](https://urlscan.io/docs/api#search) to search for scans for `tenzir.com` and get the first result.

```tql
from {}
http "https://urlscan.io/api/v1/search?q=tenzir.com"
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

### Keeping input context

Frequently, the purpose of making real-time requests in a pipeline is to enrich
the incoming data with additional context. In these cases, we want to keep the
original event around. This can be done simply by specifying the
`response_field` and `metadata_field` options as appropriate.

E.g. in the above example, let's assume we had some initial context that we want
to keep around:

```tql
from { ctx: {severity: "HIGH"}, domain: "tenzir.com", ip: 0.0.0.0 }
http "https://urlscan.io/api/v1/search?q=" + domain, response_field=scan
scan.results = scan.results[0]
```

```tql
{
  ctx: {
    severity: "HIGH",
  },
  domain: "tenzir.com",
  ip: 0.0.0.0,
  scan: {
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
    took: 88,
    has_more: false,
  },
}
```

### Paginate an API

We can utilize the `sort` and `has_more` fields to get
more pages from the API.

```tql
let $URL = "https://urlscan.io/api/v1/search?q=example.com"
from {}
http $URL, paginate=(x => $URL + "&search_after=" + results.last().sort.first() + "," + results.last().sort.last().slice(begin=1, end=-1) if has_more?)
head 10
```

Here we construct the next url for pagination by extracting values from the
responses.

The query parameter `search_after` expects the two values from the
`sort` key in the response to be joined with a `,`. Thus forming a URL like
`https://urlscan.io/api/v1/search?q=example.com&search_after=1747796723608,0196f0cd-6fda-761a-81a6-ae1b18914e61`.

The `if has_more?` ensures pagination only continues till we have a `has_more`
field that is `true`.

Additionally, we limit the maximum pages by a simple `head 10`.

## See Also

[`from_http`](/reference/operators/from_http)
