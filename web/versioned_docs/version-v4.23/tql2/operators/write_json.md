# write_json

Transforms the input event stream to JSON byte stream.

```tql
write_json [ndjson=bool, color=bool]
```

## Description

Transforms the input event stream to JSON byte stream.

### `ndjson = bool (optional)`

Whether to write newline-delimited JSON output (NDJSON/JSONL).

### `color = bool (optional)`

Colorize the output.

## Examples

### Convert a YAML stream into a JSON file

```tql
load_file "input.yaml"
read_yaml
write_json
save_file "output.json"
```
