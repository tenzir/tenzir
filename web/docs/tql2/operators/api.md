# api

Use Tenzir's REST API directly from a pipeline.

```
api endpoint:str [request_body=str]
```

## Description

The `api` operator interacts with Tenzir's REST API without needing to spin up a
web server, making all APIs accessible from within pipelines.

### `endpoint`

The endpoint to request, e.g., `/pipeline/list` to list all pipelines created
through the `/pipeline/create` endpoint.

Tenzir's [REST API specification](/api) lists all available endpoints.

### `request-body`

A single string containing the JSON request body to send with the request.

## Examples

List all running pipelines:

```
api "/pipeline/list"
```

Create a new pipeline and start it immediately.

XXX: Single-quote literals?
```
api "/pipeline/create" request_body='{"name": "Suricata Import", "definition": "from file /tmp/eve.sock read suricata", "autostart": {"created": true}}'
```
