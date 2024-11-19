# write_ndjson

Transforms the input event stream to Newline-Delimited JSON byte stream.

```tql
write_ndjson [color=bool]
```

## Description

Transforms the input event stream to Newline-Delimited JSON byte stream.
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
write_ndjson
save_file "output.json"
```
