---
sidebar_custom_props:
  operator:
    sink: true
---

# to-splunk

Sends events to a [Splunk HEC](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector)

:::tip Consider the TQL2 `to_splunk` operator
The TQL2 [`to_splunk` operator](../tql2/operators/to_splunk.md) providing
multiple additional options. It allows you to set Splunk's `index` and
`sourcetype` fields.
:::

## Synopsis

```
to-splunk <url> <hec_token> [--host <string>] [--source <string>]
       [--tls-no-verify] [--print-nulls] [--max-content-length <int>]
       [--send-timeout <duration>] [--no-compress]
```

## Description

The `splunk` sends events to a [Splunk HEC endpoint](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector).
Events are send as a JSON object in the `event` field, with Splunk `sourcetype` `_json`.

The operator aggregates multiple events to send as a single message. The size
and timeout can be configured.

### `<url>`

The address of the Splunk indexer.

### `<hec_token>`

A [Splunk HEC token](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector#Create_an_Event_Collector_token_on_Splunk_Cloud_Platform) used for authorization.

### `--host <string>`

An optional value for the [Splunk `host`](https://docs.splunk.com/Splexicon:Host).

### `--source <string>`

An optional value for the [Splunk `source`](https://docs.splunk.com/Splexicon:Source).

### `--tls-no-verify`

Disable TSL certificate verification.

### `--include-nulls`

Include fields with null values in the transmitted event data. By default null
values are dropped before sending to Splunk.

### `--max-content-length <int>`

The maximum size of the uncompressed message body. A message may consist of multiple events.
If a single event is larger than this limit, it is dropped and a warning is emitted.
The default is `5Mi`.

This corresponds with Splunk's [`max_content_length`](https://docs.splunk.com/Documentation/Splunk/9.3.1/Admin/Limitsconf#.5Bhttp_input.5D) option. Be aware that [Splunk Cloud has a default of `1MB`](https://docs.splunk.com/Documentation/SplunkCloud/9.2.2406/Service/SplunkCloudservice#Using_HTTP_Event_Collector_.28HEC.29)
for `max_content_length`.

### `--buffer-timeout <duration>`

The maximum amount of time for which the `splunk` operator aggregates messages
before sending them out to Splunk.

The default is `5s`.

### `--no-compress`

Disable compression for the message body. By default, the operator compresses
events using gzip.

## Examples
```
from "example.yaml"
| to-splunk "https://localhost:8088" "example-token-1234"
```
