---
title: write_json
category: Printing
example: 'write_json'
---

Transforms the input event stream to a JSON byte stream.

```tql
write_json [strip=bool, color=bool, arrays_of_objects=bool,
            strip_null_fields=bool, strip_nulls_in_lists=bool,
            strip_empty_records=bool, strip_empty_lists=bool]
```

## Description

Transforms the input event stream to a JSON byte stream.

:::tip[Newline-Delimited JSON (NDJSON)]
Use [`write_ndjson` operator](/reference/operators/write_ndjson) to write Newline-Delimited JSON.
:::

### `strip = bool (optional)`

Enables all `strip_*` options.

Defaults to `false`.

### `color = bool (optional)`

Colorize the output.

Defaults to `false`.

### `arrays_of_objects = bool (optional)`

Prints the input as a single array of objects, instead of as separate objects.

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

### Convert a YAML stream into a JSON file

```tql
load_file "input.yaml"
read_yaml
write_json
save_file "output.json"
```

### Strip null fields

```tql
from { yes: 1, no: null}
write_json strip_null_fields=true
```
```json
{
  "yes": 1
}
```

## See Also

[`parse_json`](/reference/functions/parse_json),
[`print_json`](/reference/functions/print_json),
[`read_json`](/reference/operators/read_json),
[`write_tql`](/reference/operators/write_tql)
