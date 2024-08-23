# write_json

Transforms the input event stream to JSON byte stream.

```tql
write_json [ndjson=bool, color=bool]
```

## Description

Transforms the input event stream to JSON byte stream.

### `ndjson = bool (optional)`

Whether to write Newline-Delimited JSON.

### `color = bool (optional)`

## Examples

Convert a YAML stream into JSON and write it to stdout.

```tql
export
read_yaml
write_json
```
