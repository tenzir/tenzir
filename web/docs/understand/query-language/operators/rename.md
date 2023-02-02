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

## YAML Syntax Example

:::info Deprecated
The YAML syntax is deprecated since VAST v3.0, and will be removed in a future
release. Please use the pipeline syntax instead.
:::

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
