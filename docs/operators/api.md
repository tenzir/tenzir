---
title: api
category: Internals
example: 'api "/pipeline/list"'
---

Use Tenzir's REST API directly from a pipeline.

```tql
api endpoint:string, [request_body:string]
```

## Description

The `api` operator interacts with Tenzir's REST API without needing to spin up a
web server, making all APIs accessible from within pipelines.

### `endpoint: string`

The endpoint to request, e.g., `/pipeline/list` to list all managed pipelines.

Tenzir's [REST API specification](/reference/node/api) lists all available endpoints.

### `request_body: string (optional)`

A single string containing the JSON request body to send with the request.

## Examples

### List all running pipelines

```tql
api "/pipeline/list"
```

### Create a new pipeline and start it immediately

```tql
api "/pipeline/create", {
  name: "Suricata Import",
  definition: "from file /tmp/eve.sock read suricata",
  autostart: { created: true },
}
```

## See Also

[`openapi`](/reference/operators/openapi),
[`serve`](/reference/operators/serve)
