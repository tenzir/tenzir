# write_yaml

Transforms the input event stream to YAML byte stream.

```tql
write_yaml
```

## Description

Transforms the input event stream to YAML byte stream.

## Examples

### Convert a JSON file into a YAML file

```tql
load_file "input.json"
read_json
write_yaml
save_file "output.yaml"
```

## See Also

[`read_yaml`](read_yaml.mdx), [`print_yaml`](../functions/print_yaml.md)
