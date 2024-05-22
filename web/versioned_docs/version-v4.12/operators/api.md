---
sidebar_custom_props:
  operator:
    source: true
---

# api

Use Tenzir's REST API directly from a pipeline.

## Synopsis

```
api <endpoint> [<request-body>]
```

## Description

The `api` operator interacts with Tenzir's REST API without needing to spin up a
web server, making all APIs accessible from within pipelines.

:::info OpenAPI
Visit [Tenzir's REST API specification](/api) to see a list of all available
endpoints.
:::

### `<endpoint>`

The endpoint to request, e.g., `/pipeline/list` to list all pipelines created
through the `/pipeline/create` endpoint.

### `[<request-body>]`

A single string containing the JSON request body to send with the request.

## Examples

List all running pipelines:

```
api /pipeline/list
```

Create a new pipeline and start it immediately.

```
api /pipeline/create '{"name": "Suricata Import", "definition": "from file /tmp/eve.sock read suricata", "autostart": {"created": true}}'
```
