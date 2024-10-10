# enumerate

Add a column with row numbers.

```
enumerate out:field
```

## Description

The `enumerate` operator adds a new column with row numbers to the beginning
of the input record.

### `out`

Sets the name of the output field.

Defaults to `#`.

## Examples

Enumerate the input by prepending row numbers:

```
from file eve.json read suricata | select event_type | enumerate | write_json
```

```json
{"#": 0, "event_type": "alert"}
{"#": 1, "event_type": "flow"}
{"#": 2, "event_type": "flow"}
{"#": 3, "event_type": "http"}
{"#": 4, "event_type": "alert"}
{"#": 5, "event_type": "http"}
{"#": 6, "event_type": "flow"}
{"#": 7, "event_type": "fileinfo"}
{"#": 8, "event_type": "flow"}
{"#": 9, "event_type": "flow"}
```

Use `index` as field name instead of the default:

```
enumerate index
```
