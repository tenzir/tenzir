# from_http

Sends and receives HTTP/1.1 requests.

## Synopsis

```tql
from_http url:string, [method=string, payload=string, server=bool, tls=bool,
          certfile=string, keyfile=string, password=string, responses=record,
          max_request_size=int]
```

## Description

The `from_http` operator issues HTTP requests or spins up an HTTP/1.1 server on
a given address and forwards received requests as events.

### `url: string`

URL to listen on or to connect to.

Must have the form `<host>:<port>`.

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

Defaults to `get`.

### `payload = string (optional)`

Payload to send with the HTTP request.

### `server = bool (optional)`

Whether to spin up an HTTP server or act as an HTTP client.

Defaults to `false`, i.e., the HTTP client.

### `tls = bool (optional)`

Enables TLS.

Defaults to `false`.

### `certfile = string (optional)`

Path to the client certificate. Required if `tls` is `true`.

### `keyfile = string (optional)`

Path to the key for the client certificate. Required if `tls` is `true`.

### `password = string (optional)`

Password for keyfile.

### `responses = record (optional)`

Specify custom responses for endpoints on the server. For example,

```tql
responses = {
    "/resource/create": { code: 200, content_type: "text/html", payload: "Created!" },
    "/resource/delete": { code: 401, content_type: "text/html", payload: "Unauthorized!" }
}
```

creates two special routes on the server with different responses.

Requests to an unspecified endpoint are responded with HTTP Status `200 OK`.

### `max_request_size = int (optional)`

The maximum size of an incoming request to accept.

Defaults to `10Mib`.

## Examples

### Listen on port 8080

Spin up a server with:

```tql
from_http "0.0.0.0:8080", server=true
body = body.string().parse_json()
```

and send a curl request at it via:

```sh
echo '{"key": "value"}' | gzip | curl localhost:8080 --data-binary @- -H 'Content-Encoding: gzip'
```

and see it on the Tenzir side, parsed and decompressed(!):

```tql
{
  headers: {
    "Host": "localhost:8080",
    "User-Agent": "curl/8.13.0",
    "Accept": "*/*",
    "Content-Encoding": "gzip",
    "Content-Length": "37",
    "Content-Type": "application/x-www-form-urlencoded",
  },
  path: "/",
  method: "post",
  version: "HTTP/1.1",
  body: {
    key: "value",
  },
}
```

and then strip out all the HTTP framing with [`select`](select.md):

```tql
select parsed=body
```

```tql
{ parsed: { key: "value" } }
```
