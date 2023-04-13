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
