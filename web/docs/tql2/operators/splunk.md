# splunk

Sends events to a [Splunk HEC](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector).

```tql
splunk url:str, hec_token=str,
           [host=str, source=str, sourcetype=expr, index=expr,
            tls_no_verify=bool, print_nulls=bool,
            max_content_length=int, send_timeout=duration, compress=bool]
```

## Description

The `splunk` operator sends events to a Splunk instance using the [HTTP Event Collector (HEC)](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector) protocol. Events are sent as JSON.

The operator aggregates multiple events to send as a single message. The size
and timeout can be configured.

### `url: str`

The address of the Splunk indexer.

### `hec_token = str`

A [Splunk HEC token](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector#Create_an_Event_Collector_token_on_Splunk_Cloud_Platform) used for authorization.

### `host = str (optional)`

An optional value for the [Splunk `host`](https://docs.splunk.com/Splexicon:Host).

### `source = str (optional)`

An optional value for the [Splunk `source`](https://docs.splunk.com/Splexicon:Source).

### `sourcetype = expr (optional)`

An optional expression for [Splunk's `sourcetype`](https://docs.splunk.com/Splexicon:Sourcetype).
This can be any expression that evaluates to `string`.
The default is the string `"_json"`. You can use this to set the Splunk
`sourcetype` based on each event, by providing a filename instead.

Be aware that the Splunk HEC silently drops events with an invalid `sourcetype`

### `index = expr (optional)`

An optional expression for the [Splunk `index`](https://docs.splunk.com/Splexicon:Index).
This can be any expression that evaluates to `string`.
If this option is not given, the `index` is omitted, effectively using the Splunk
default index. You can use this to set the Splunk `index` based on each event,
by providing a filename instead.

Be aware that the Splunk HEC silently drops events with an invalid `index`

### `tls_no_verify = bool (optional)`

Disable TSL certificate verification.

### `include_nulls = bool (optional)`

Include fields with null values in the transmitted event data. By default they
are dropped before sending to splunk

### `max_content_length = int (optional)`

The maximum size of the message body. A message may consist of multiple events.
If a single event is larger than this limit, it is dropped and a warning is emitted.
The default is `5MB`.

This corresponds with Splunk's [`max_content_length`](https://docs.splunk.com/Documentation/Splunk/9.3.1/Admin/Limitsconf#.5Bhttp_input.5D) option. Be aware that [Splunk Cloud has a default of `1MB`](https://docs.splunk.com/Documentation/SplunkCloud/9.2.2406/Service/SplunkCloudservice#Using_HTTP_Event_Collector_.28HEC.29)
for `max_content_length`.

### `send_timeout = duration (optional)`

The maximum amount of time for which the `splunk` operator aggregates messages
before sending them out to Splunk.

### `compress = bool (optional)`

Whether to compress the message body using standard gzip. Compression is enabled
by default.

## Examples
```tql
load_file "example.yaml"
parse_yaml
splunk "https://localhost:8088", hec_token="example-token-1234"
```
