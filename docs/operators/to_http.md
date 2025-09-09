---
title: to_http
category: Sink
example: 'to_http "https://api.example.com/data"'
---

Sends HTTP/1.1 requests as a sink operator, discarding responses.

```tql
to_http url:string, [method=string, body=record|string|blob, encode=string,
        headers=record, parallel=int, tls=bool, certfile=string, keyfile=string,
        password=string, connection_timeout=duration, max_retry_count=int,
        retry_delay=duration]
```

## Description

The `to_http` operator is a sink that issues HTTP/1.1 requests but discards the responses. Unlike the [`http`](/reference/operators/http) operator which forwards response data, `to_http` is designed for scenarios where you only need to send data via HTTP without processing the response, such as webhooks, logging, or notifications.

As a sink operator, `to_http` produces no output events and is typically used at the end of a pipeline to send processed data to external HTTP endpoints.

### `url: string`

URL to send requests to.

### `method = string (optional)`

One of the following HTTP methods to use:

- `get`
- `head`
- `post`
- `put`
- `del`
- `connect`
- `options`
- `trace`

Defaults to `get`, or `post` if `body` is specified.

### `body = blob|record|string (optional)`

Body to send with the HTTP request.

If the value is a `record`, then the body is encoded according to the `encode`
option and an appropriate `Content-Type` is set for the request.

### `encode = string (optional)`

Specifies how to encode `record` bodies. Supported values:

- `json`
- `form`

Defaults to `json`.

### `headers = record (optional)`

Record of headers to send with the request.

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

## Examples

### Send alerts to a webhook

Send security alerts to a Slack webhook:

```tql
from suricata
where event_type == "alert"
to_http "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
        body={text: "Alert: " + alert.signature + " from " + src_ip}
```

### POST JSON data to an API

Send processed events as JSON to an external API:

```tql
from zeek
where _path == "conn"
extend {
  event_type: "connection",
  timestamp: ts,
  source: orig_h,
  destination: resp_h
}
to_http "https://api.example.com/events", method="post",
        headers={Authorization: "Bearer " + $API_TOKEN}
```

### Send form data with custom headers

Send data as form-encoded with authentication:

```tql
from csv
to_http "https://api.example.com/submit", method="post",
        encode="form",
        headers={
          "Content-Type": "application/x-www-form-urlencoded",
          "X-API-Key": $API_KEY
        }
```

### Parallel requests with retry logic

Send data to multiple endpoints with error handling:

```tql
from json
to_http "https://backup-api.example.com/data",
        parallel=5,
        max_retry_count=3,
        retry_delay=2s,
        connection_timeout=10s
```

## Differences from `http` operator

The key differences between `to_http` and [`http`](/reference/operators/http) are:

| Feature               | `to_http`                        | `http`                                            |
| --------------------- | -------------------------------- | ------------------------------------------------- |
| **Purpose**           | Sink - send data out             | Source/Transform - fetch and process data         |
| **Output**            | No output events                 | Forwards response data as events                  |
| **Response handling** | Discarded (logged for debugging) | Parsed and forwarded                              |
| **Response fields**   | Not available                    | `response_field`, `metadata_field`, `error_field` |
| **Pagination**        | Not supported                    | Supported via `paginate`                          |
| **Parse pipeline**    | Not supported                    | Supported via `{ ... }`                           |
| **Use case**          | Webhooks, notifications, logging | API consumption, data fetching                    |

## See Also

[`http`](/reference/operators/http), [`from_http`](/reference/operators/from_http)
