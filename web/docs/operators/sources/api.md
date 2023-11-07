# api

Use Tenzir's REST API directly from within a pipeline.

## Synopsis

```
api <endpoint> [<key=value>...]
```

## Description

The `api` operator interacts with Tenzir's REST API without needing to spin up a
web server, making all APIs accessible from within pipelines.

:::info OpenAPI
Visit [Tenzir's REST API specification](/api) to see a list of all available
endpoints.
:::

### `<endpoint>`

The endpoint to query, e.g., `/pipeline/list` to list all pipelines created
through the `/pipeline/create` endpoint.

### `[<key=value>...]`

A space-separated list of key-value pairs. To set nested values, use a dot to
separate the keys.

## Examples

Create a new pipeline and start it immediately.

```
api /pipeline/create
    autostart.created=true
    "name=Suricata Import"
    "definition=from file /tmp/eve.sock read suricata | import"
```

List all running pipelines:

```
api /pipeline/list
```
