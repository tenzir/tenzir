---
sidebar_custom_props:
  operator:
    sink: true
---

# splunk

Sends events to a [Splunk HEC](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector)

## Synopsis

```
splunk <url> <hec_token> [--host <string>] [--source <string>] [--sourcetype <string>]
       [--index <string>] [--tls-no-verify] [--print-nulls]
       [--max-content-length <int>] [--send-timeout <duration>]
```

## Description

The `splunk` sends events to a [Splunk HEC endpoint](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector).
Events are sent as JSON.

The operator aggregates multiple events to send as a single message. The size
and timeout can be configured.

### `<url>`

The address of the Splunk indexer.

### `<hec_token>`

A Splunk HEC token used for authorization.

### `--host <string>`

An optional value for the `host` field.

### `--source <string>`

An optional value for the `source` field.

### `--sourcetype <string>`

An optional value for the `sourcetype` field. If its not given, it will be set
to `_json`.

### `--index <string>`

An optional value for the `index` field.

### `--tls-no-verify`

Disable TSL certificate verification.

### `--include-nulls`

Include fields with null values in the transmitted event data. By default they
are dropped before sending to splunk

### `--max-content-length <int>`

The maximum size of the message body. A message may consist of multiple events.
If a single event is larger than this limit, it is dropped.

The default is `5MB`.

### `--send-timeout <duration>`

The maximum amount of time for which the `splunk` operator aggregates messages
before sending them out to Splunk.

The default is `5s`.

## Examples
```
from "example.yaml"
| splunk "https://localhost:8088" "example-token-1234"
```
