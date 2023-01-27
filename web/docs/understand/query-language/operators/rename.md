# rename

Renames schemas and fields according to a configured mapping.

## Parameters

- `schemas: <list>`: a list of records containing the fields `from`, and `to`
  containing the old and new schema names respectively.
- `fields: <list>`: a list of records containing the fields `from`, and `to`
  containing the old and new field names respectively.

## Example

```yaml
rename:
  schemas:
    - from: suricata.flow
      to: suricata.renamed-flow
  fields:
    - from: src_port
      to: source_port
    - from: src_ip
      to: source_address
```

## Pipeline Operator String Syntax (Experimental)

```
rename NEW=EXTRACTOR[, …]
```

### Example

Rename the `suricata.flow` schema to `my.connection`, `src_port` field to
`source_port` and `src_ip` field to `source_address`:

```
rename my.connection=:suricata.flow, source_port=src_port, source_address=src_ip
```
