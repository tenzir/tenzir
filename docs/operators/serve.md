---
title: serve
category: Internals
example: 'serve "abcde12345"'
---

Make events available under the `/serve` REST API endpoint

```tql
serve id:string, [buffer_size=int]
```

## Description

The `serve` operator bridges between pipelines and the corresponding `/serve`
[REST API endpoint](/reference/node-api).

![Serve Operator](serve.excalidraw.svg)

Pipelines ending with the `serve` operator exit when all events have been
delivered over the corresponding endpoint.

### `id: string`

An identifier that uniquely identifies the operator. The `serve`
operator errors when receiving a duplicate serve id.

### `buffer_size = int (optional)`

The buffer size specifies the maximum number of events to keep in the `serve`
operator to make them instantly available in the corresponding endpoint before
throttling the pipeline execution.

Defaults to `1Ki`.

## Examples

### Make the input available as REST API

Read a Zeek `conn.log` and make it available as `zeek-conn-logs`:

```tql
load_file "path/to/conn.log"
read_zeek_tsv
serve "zeek-conn-logs"'
```

Then fetch the first 100 events from the `/serve` endpoint:

```bash
curl \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"serve_id": "zeek-conn-logs", "continuation_token": null, "timeout": "1s", "max_events": 100}' \
  http://localhost:5160/api/v0/serve
```

This will return up to 100 events, or less if the specified timeout of 1 second
expired.

Subsequent results for further events must specify a continuation token. The
token is included in the response under `next_continuation_token` if there are
further events to be retrieved from the endpoint.

### Wait for the first event

This pipeline will produce 10 events after 3 seconds of doing nothing.

```tql
shell "sleep 3; jq --null-input '{foo: 1}'"
read_json
repeat 10
serve "slow-events"
```

```bash
curl \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"serve_id": "slow-events", "continuation_token": null, "timeout": "5s", "min_events": 1}' \
  http://localhost:5160/api/v0/serve
```

The call to `/serve` will wait up to 5 seconds for the first event from the
pipeline arriving at the serve operator, and return immediately once the first
event arrives.

## See Also

[`api`](/reference/operators/api),
[`from_http`](/reference/operators/from_http),
[`openapi`](/reference/operators/openapi)
