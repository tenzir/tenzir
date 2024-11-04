# to_splunk

Sends events to a Splunk [HTTP Event Collector (HEC)][hec].

[hec]: https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector

```tql
to_splunk
 url:str, hec_token=str,
      [host=str, source=str, sourcetype=expr, index=expr,
       tls_no_verify=bool, print_nulls=bool,
       max_content_length=int, send_timeout=duration, compress=bool]
```

## Description

The `to_splunk` operator sends events to a Splunk [HTTP Event Collector
(HEC)][hec].

The source type defaults to `_json` and the operator renders incoming events as
JSON. You can specify a different source type via the `sourcetype` option.

The operator accumulates multiple events before sending them as a single
message to the HEC endpoint. You can control the maximum message size via the
`max_content_length` and the timeout before sending all accumulated events via
the `send_timeout` option.

### `url: str`

The address of the Splunk indexer.

### `hec_token = str`

The [HEC
token](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector#Create_an_Event_Collector_token_on_Splunk_Cloud_Platform)
for authentication.

### `host = str (optional)`

An optional value for the [Splunk `host`](https://docs.splunk.com/Splexicon:Host).

### `source = str (optional)`

An optional value for the [Splunk `source`](https://docs.splunk.com/Splexicon:Source).

### `sourcetype = expr (optional)`

An optional expression for [Splunk's
`sourcetype`](https://docs.splunk.com/Splexicon:Sourcetype) that evaluates to a
`string`. You can use this to set the `sourcetype` per event, by providing a
field instead of a string.

Regardless of the chosen `sourcetype`, the event itself is passed as a json object
in `event` key of level object that is sent.

Defaults to `_json`.

### `index = expr (optional)`

An optional expression for the [Splunk
`index`](https://docs.splunk.com/Splexicon:Index) that evaluates to a `string`.

If you do not provide this option, Splunk will use the default index.

**NB**: HEC silently drops events with an invalid `index`.

### `tls_no_verify = bool (optional)`

Toggles TLS certificate verification.

### `include_nulls = bool (optional)`

Include fields with null values in the transmitted event data. By default, the
operator drops all null values to save space.

### `max_content_length = int (optional)`

The maximum size of the message uncompressed body in bytes. A message may consist of multiple events.
If a single event is larger than this limit, it is dropped and a warning is emitted.

This corresponds with Splunk's
[`max_content_length`](https://docs.splunk.com/Documentation/Splunk/9.3.1/Admin/Limitsconf#.5Bhttp_input.5D)
option. Be aware that [Splunk Cloud has a default of
`1MB`](https://docs.splunk.com/Documentation/SplunkCloud/9.2.2406/Service/SplunkCloudservice#Using_HTTP_Event_Collector_.28HEC.29)
for `max_content_length`.

Defaults to `5Mi`.

### `buffer_timeout = duration (optional)`

The maximum amount of time for which the operator accumulates messages before
sending them out to the HEC endpoint as a single message.

Defaults to `5s`.

### `compress = bool (optional)`

Whether to compress the message body using standard gzip.

Defaults to `true`.

## Examples

### Send a JSON file to a HEC endpoint

```tql
load_file "example.json"
read_json
to_splunk "https://localhost:8088", hec_token="example-token-1234"
```
