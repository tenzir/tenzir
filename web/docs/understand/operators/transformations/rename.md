# rename

Renames schemas and fields according to a configured mapping.

## Synopsis

```
rename NAME=EXTRACTOR[, …]
```

### Names

An assignment for a new name from an existing extractor. Use type extractors to
rename schemas.

## Example

Rename the `suricata.flow` schema to `my.connection`, `src_port` field to
`source_port` and `src_ip` field to `source_address`:

```
rename my.connection=:suricata.flow, source_port=src_port, source_address=src_ip
```
