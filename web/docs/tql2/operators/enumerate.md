# enumerate

Add a column with row numbers.

```tql
enumerate [out:field]
```

## Description

The `enumerate` operator adds a new column with row numbers to the beginning
of the input record.

### `out: field (optional)`

Sets the name of the output field.

Defaults to `#`.

## Examples

Enumerate the input by prepending row numbers:

```tql
load_file "eve.json"
read_suricata
select event_type
enumerate
```

<details>
<summary>Example Output</summary>

```json
{"#": 0, "event_type": "alert"}
{"#": 1, "event_type": "flow"}
{"#": 2, "event_type": "flow"}
{"#": 3, "event_type": "http"}
{"#": 4, "event_type": "alert"}
{"#": 5, "event_type": "http"}
{"#": 6, "event_type": "flow"}
```
</details>

Use `index` as field name instead of the default:

```tql
enumerate index
```

<details>
<summary>Example Output</summary>

```json
{"index": 0, "event_type": "alert"}
{"index": 1, "event_type": "flow"}
{"index": 2, "event_type": "flow"}
{"index": 3, "event_type": "http"}
{"index": 4, "event_type": "alert"}
{"index": 5, "event_type": "http"}
{"index": 6, "event_type": "flow"}
```
</details>
