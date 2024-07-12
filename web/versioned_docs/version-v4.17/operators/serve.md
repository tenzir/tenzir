---
sidebar_custom_props:
  operator:
    sink: true
---

# serve

Make events available under the [`/serve` REST API
endpoint](/api#/paths/~1serve/post).

## Synopsis

```
serve [--buffer-size <buffer-size>] <serve-id>
```

## Description

The `serve` operator bridges between pipelines and the corresponding `/serve`
[REST API endpoint](/api#/paths/~1serve/post):

![Serve Operator](serve.excalidraw.svg)

Pipelines ending with the `serve` operator exit when all events have been
delivered over the corresponding endpoint.

### `--buffer-size <buffer-size>`

The buffer size specifies the maximum number of events to keep in the `serve`
operator to make them instantly available in the corresponding endpoint before
throttling the pipeline execution.

Defaults to `64Ki`.

### `<serve-id>`

The serve id is an identifier that uniquely identifies the operator. The `serve`
operator errors when receiving a duplicate serve id.

## Examples

### Read a Zeek conn.log, 100 events at a time:

```bash
tenzir 'from file path/to/conn.log read zeek-tsv | serve zeek-conn-logs'
```

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

### Wait for an initial event

This pipeline will produce 10 events after 3 seconds of doing nothing.

```bash
tenzir "shell \"sleep 3; jq --null-input '{foo: 1}'\" | read json | repeat 10 | serve slow-events"
```

```bash
curl \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"serve_id": "slow-events", "continuation_token": null, "timeout": "5s", "min_events": 1}' \
  http://localhost:5160/api/v0/serve
```

The call to `/serve` will wait up to 5 seconds for the first event from the pipeline arriving at the serve operator,
and return immediately once the first event arrives.
