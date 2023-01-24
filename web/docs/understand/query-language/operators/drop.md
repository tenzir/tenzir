# drop

Drops individual fields having the configured extractors from the input or
entire schemas.

The `drop` operator is the dual to [`select`](select), which selects a given set
of fields from the output.

## Parameters

- `fields: [string]`: The extractors of fields to drop.
- `schemas: [string]`: The names of schemas to drop.

## Example

```yaml
drop:
  fields:
    # Remove the source_ip and dest_ip columns if they exist
    - source_ip
    - dest_ip
  schemas:
    # Drop all suricata.dns events in their entirety
    - suricata.dns
```

## Pipeline Operator String Syntax (Experimental)

```
drop FIELD/SCHEMA[, …]
```
### Example
```
drop source_ip, dest_ip, suricata.dns
```
