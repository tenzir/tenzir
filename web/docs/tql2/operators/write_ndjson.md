# write_ndjson

Transforms the input event stream to a Newline-Delimited JSON byte stream.

```tql
write_ndjson [strip=bool, color=bool
              strip_null_fields=bool, strip_nulls_in_lists=bool
              strip_empty_records=bool, strip_empty_lists=bool]
```

## Description

Transforms the input event stream to a Newline-Delimited JSON byte stream.

### `strip = bool (optional)`

Enables all `strip_*` options.

### `color = bool (optional)`

Colorize the output.

### `strip_null_fields = bool (optional)`

Strips all fields with a `null` value from records.

### `strip_nulls_in_lists = bool (optional)`

Strips all `null` value to be from lists.

### `strip_empty_records = bool (optional)`

Strips empty records from the output, including those that only became empty
by stripping nulls.

### `strip_empty_lists = bool (optional)`

Strips empty lists from the output, including those that only became empty
by stripping nulls.

## Examples

### Convert a YAML stream into a JSON file

```tql
load_file "input.yaml"
read_yaml
write_ndjson
save_file "output.json"
```
