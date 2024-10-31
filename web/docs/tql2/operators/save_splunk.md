# save_splunk

Sends events to a [Splunk HEC](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector)

```tql
save_splunk url:str, hec_token=str,
           [host=str, source=str,
            sourcetype=str, sourcetype_field=field,
            index=str, index_field=field,
            tls_no_verify=bool, print_nulls=bool]
```

## Description

The `splunk` sends events to a [Splunk HEC endpoint](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/UsetheHTTPEventCollector).
Events are sent as JSON.

### `url: str`

The address of the Splunk indexer.

### `hec_token = str`

A Splunk HEC token used for authorization.

### `host = str (optional)`

An optional value for the `host` field.

### `source = str (optional)`

An optional value for the `source` field.

### `sourcetype = str (optional)`

An optional value for the `sourcetype` field. If its not given, it will be set
to `_json`.

### `sourcetype_field = field (optional)`

If the `sourcetype_field` is present in the input data, the `sourcetype` will be
set to the value of that field. This takes precedence over the `sourcetype` option.

### `index = str (optional)`

An optional value for the `index` field.

### `index_field = field (optional)`

If the `index_field` is present in the input data, the `index` will be set to
the value of that field. This takes precedence over the `index` option.

### `tls_no_verify = bool (optional)`

Disable TSL certificate verification.

### `include_nulls = bool (optional)`

Include fields with null values in the transmitted event data. By default they
are dropped before sending to splunk

## Examples
```tql
load_file "example.yaml"
parse_yaml
save_splunk "https://localhost:8088", hec_token="example-token-1234"
```
