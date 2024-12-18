# to_opensearch

Sends events to an OpenSearch-compatible Bulk API.

```tql
to_opensearch url:string, action=string, [index=string, id=string, doc=record,
    user=string, passwd=string, skip_peer_verification=bool, cacert=string,
    certfile=string, keyfile=string, include_nulls=bool, max_content_length=int,
    buffer_timeout=duration, compress=bool]
```

## Description

The `to_opensearch` operator sends events to a OpenSearch-compatible API.

The operator accumulates multiple events before sending them as a single
request. You can control the maximum request size via the
`max_content_length` and the timeout before sending all accumulated events via
the `send_timeout` option.

### `url: string`

The URL of the API endpoint.

### `action = string`

An expression for the action that evalutes to a `string`.

Supported actions:
- `create`
- `delete`
- `index`
- `update`
- `upsert`

### `index = expr (optional)`

An optional expression for the index that evaluates to a `string`.

Must be provided if the `url` does not have an index.

### `id = expr (optional)`

The `id` of the document to act on.

Must be provided when using the `delete` and `update` actions.

### `doc = record (optional)`

The document to serialize.

Defaults to `this`.

### `user = string (optional)`

Optional user for HTTP Basic Authentication.

### `passwd = string (optional)`

Optional password for HTTP Basic Authentication.

### `skip_peer_verification = bool (optional)`

Toggles TLS certificate verification.

Defaults to `false`.

### `cacert = string (optional)`

Path to the CA certificate used to verify the server's certificate.

### `certfile = string (optional)`

Path to the client certificate.

### `keyfile = string (optional)`

Path to the key for the client certificate.

### `include_nulls = bool (optional)`

Include fields with null values in the transmitted event data. By default, the
operator drops all null values to save space.

Defaults to `false`.

### `max_content_length = int (optional)`

The maximum size of the message uncompressed body in bytes. A message may consist of multiple events.
If a single event is larger than this limit, it is dropped and a warning is emitted.

Defaults to `5Mi`.

### `buffer_timeout = duration (optional)`

The maximum amount of time for which the operator accumulates messages before
sending them out to the HEC endpoint as a single message.

Defaults to `5s`.

### `compress = bool (optional)`

Whether to compress the message body using standard gzip.

Defaults to `true`.

## Examples

### Send events from a JSON file

```tql
load_file "example.json"
read_json
to_opensearch "localhost:9200", action="create", index="main"
```
