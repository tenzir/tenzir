# put

Returns new events with the specified fields only.

## Synopsis

```
put FIELD[=OPERAND] [, …]
```

### Fields

The names of fields in the output data. If the right-hand side of the assignment
is omitted, the field name is implicitly used as an extractor. If multiple
fields match the extractor, the first matching field is used in the output. If
no fields match, `null` is assigned instead.

### Example

Given this input event (JSON) and this pipeline:

```json
{
  "src_ip": "10.0.0.1",
  "dest_ip": "192.168.0.1"
  "src_port": 80,
  "payload": "foobarbaz"
}
```

```c
put source_ip=src_ip, dest_ip, dest_port, payload="REDACTED"
```

The pipeline will always result in events with the four fields `source_ip`
(renamed from `src_ip`), `dest_ip` (unchanged), `dest_port` (`null` as the field
is not available in the input data), and `payload` (set to the fixed value
`"REDACTED"`) in that order.

```json
{
  "source_ip": "10.0.0.1",
  "dest_ip": "192.168.0.1"
  "dest_port": null,
  "payload": "REDACTED"
}
```
