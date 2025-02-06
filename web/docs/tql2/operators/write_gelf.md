# write_gelf

Transforms the input event stream to a NULL-Delimited JSON byte stream.

```tql
write_gelf [strip=bool, color=bool
            strip_null_fields=bool, strip_nulls_in_lists=bool
            strip_empty_records=bool, strip_empty_lists=bool]
```

## Description

Transforms the input event stream to a NULL-Delimited JSON byte stream.

### `strip = bool (optional)`

Enables all `strip_*` options.

Defaults to `false`.

### `color = bool (optional)`

Colorize the output.

Defaults to `false`.

### `strip_null_fields = bool (optional)`

Strips all fields with a `null` value from records.

Defaults to `false`.

### `strip_nulls_in_lists = bool (optional)`

Strips all `null` values from lists.

Defaults to `false`.

### `strip_empty_records = bool (optional)`

Strips empty records, including those that only became empty
by stripping.

Defaults to `false`.

### `strip_empty_lists = bool (optional)`

Strips empty lists, including those that only became empty
by stripping.

Defaults to `false`.

## Examples

### Convert a YAML stream into a GELF file

```tql
load_file "input.yaml"
read_yaml
write_gelf
save_file "output.gelf"
```

### Strip null fields

```tql
from { key: "one" }, { key: "two" }
write_gelf
```
```json
{"key":"one"}\0{"key":"two"}
```
