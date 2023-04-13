# drop

Drops individual fields having the configured extractors from the input or
entire schemas.

The `drop` operator is the dual to [`select`](select), which selects a given set
of fields from the output.

## Synopsis

```
drop EXTRACTORS[, …]
```

### Extractors

The extractors of the fields or schemas to drop.

## Example

Drop the fields `source_ip` and `dest_ip`, and all schemas of type
`suricata.dns`:

```
drop source_ip, dest_ip, :suricata.dns
```

## YAML Syntax Example

:::info Deprecated
The YAML syntax is deprecated since VAST v3.0, and will be removed in a future
release. Please use the pipeline syntax instead.
:::

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
