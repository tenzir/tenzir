# put

Assign new values to given fields in the input data, or add new fields if the
assigned-to field did not exist.

## Synopsis

```
put EXTRACTORS=VALUE[, …]
```

### Extractors

The extractors of fields to replace with a new value. For extractors that do not
match any fields in the input events, the operator adds a new field at the end.

### Example

Given this input event (JSON) and this pipeline:

```json
{
  "source_ip": "10.0.0.1",
  "dest_ip": "192.168.0.1"
}
```

```c
put source_ip="REDACTED", note="masked source IP address for GDPR-compliance"
```

The pipeline will replace the field `source_ip` with a fixed value, and add a
new field `note` at the end, leading to this output:

```json
{
  "source_ip": "REDACTED",
  "dest_ip": "192.168.0.1",
  "note": "masked source IP address for GDPR-compliance"
}
```
