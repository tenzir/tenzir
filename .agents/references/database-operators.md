# Database operator contract

Use this reference when adding or changing database-backed `from_*` and `to_*`
operators.

## Goal

Database operators should expose a small common API across backends. Prefer the
same argument names and semantics unless a backend has a strong reason not to
support them.

## Connection arguments

Use these names consistently:

- `uri`
- `host`
- `port`
- `user`
- `password`
- `tls`

### Rules

- `uri` is an alternative to `host`/`port`/`user`/`password`.
- `uri` must be mutually exclusive with explicit connection arguments.
- `tls` keeps the usual Tenzir semantics and may be `bool` or `record`.
- Credentials should continue to flow through Tenzir secrets.

## Source operators

Database source operators should use this common query surface:

- `table`
- `sql`
- `live`
- `tracking_column`

### Rules

- `table` and `sql` are mutually exclusive.
- At least one of `table` or `sql` must be set.
- `live` is only valid with `table`.
- `tracking_column` is only valid with `live=true`.
- If a backend supports polling or incremental reads, use `live` and
  `tracking_column` for that API instead of inventing backend-specific names.

## Destination operators

Database destination operators should use this common write surface:

- `table`
- `mode`
- `primary`

### Rules

- `table` names the destination relation.
- `mode` and `primary` should follow the `to_clickhouse` contract.
- Do not introduce `live` or `tracking_column` on the destination side.

## Explicit non-goals for the common API

These should not be part of the shared baseline:

- `database`
- `show`

### Use instead

- Select a database through the URI or by qualifying `table` as `database.table`.
- Fetch metadata through `sql`, e.g. `SHOW ...`, `DESCRIBE ...`, or queries
  against system catalogs.

## Review checklist

When reviewing a database operator, check that it:

- uses the canonical argument names above
- keeps mutual exclusivity rules intact
- does not add backend-specific aliases without a strong reason
- documents any unsupported common arguments explicitly
- stays aligned with existing operators where semantics already exist
