# write_json

Transforms the input event stream to JSON byte stream.

```tql
write_json [color=bool]
```

## Description

Transforms the input event stream to JSON byte stream.
:::tip Newline-Delimited JSON
You can use the [`write_ndjson` operator](write_ndjson.md) to write Newline-Delimited JSON.
:::
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
