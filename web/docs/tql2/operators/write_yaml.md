# write_yaml

Transforms the input event stream to YAML byte stream.

```tql
write_yaml
```

## Description

Transforms the input event stream to YAML byte stream.

## Examples

### Convert a JSON stream into a YAML file

```tql
load_file "input.json"
read_json
write_yaml
save_file "output.yaml"
```
