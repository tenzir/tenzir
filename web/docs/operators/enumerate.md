---
sidebar_custom_props:
  operator:
    transformation: true
---

# enumerate

Prepend a column with row numbers.

## Synopsis

```
enumerate [<field>]
```

## Description

The `enumerate` operator prepends a new column with row numbers to the beginning
of the input record.

### `<field>`

Sets the name of the output field.

Defaults to `#` to avoid conflicts with existing field names.

## Examples

Enumerate the input by prepending row numbers:

```
from file eve.json read suricata | select event_type | enumerate | write json
```

```json
{"#": 0, "event_type": "alert"}
{"#": 0, "event_type": "flow"}
{"#": 1, "event_type": "flow"}
{"#": 0, "event_type": "http"}
{"#": 1, "event_type": "alert"}
{"#": 1, "event_type": "http"}
{"#": 2, "event_type": "flow"}
{"#": 0, "event_type": "fileinfo"}
{"#": 3, "event_type": "flow"}
{"#": 4, "event_type": "flow"}
```

Use `index` as field name instead of the default:

```
enumerate index
```
