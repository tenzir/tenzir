# api

Use Tenzir's REST API directly from a pipeline.

```tql
api endpoint:str [request_body=str]
```

## Description

The `api` operator interacts with Tenzir's REST API without needing to spin up a
web server, making all APIs accessible from within pipelines.

### `endpoint: str`

The endpoint to request, e.g., `/pipeline/list` to list all pipelines created
through the `/pipeline/create` endpoint.

Tenzir's [REST API specification](/api) lists all available endpoints.

### `request_body = str (optional)`

A single string containing the JSON request body to send with the request.

## Examples

List all running pipelines:

```tql
api "/pipeline/list"
```

Create a new pipeline and start it immediately.

```tql
api "/pipeline/create" request_body=r#"{"name": "Suricata Import", "definition": "from file /tmp/eve.sock read suricata", "autostart": {"created": true}}"#
```
