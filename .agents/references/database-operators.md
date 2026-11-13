# Database operator contract

Use this reference when adding or changing database-backed `from_*` and `to_*`
operators.

## Goal

Database operators should expose a small common API across backends. Prefer the
same argument names and semantics, and document any backend-specific shape
clearly when a backend needs to diverge.

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
- `uri` and explicit connection arguments are mutually exclusive.
- `tls` keeps the usual Tenzir semantics and may be `bool` or `record`.
- Credentials should continue to flow through Tenzir secrets.
- Database selection should happen through the URI or by qualifying `table` as
  `database.table`.

## Source operators

Database source operators should use this common query surface:

- `table`
- `sql`
- `live`
- `tracking_column`

### Rules

- `table` and `sql` are mutually exclusive.
- At least one of `table` or `sql` must be set.
- `live` pairs with `table`.
- `tracking_column` pairs with `live=true`.
- Metadata queries should use `sql`, e.g. `SHOW ...`, `DESCRIBE ...`, or
  queries against system catalogs.
- If a backend supports polling or incremental reads, use `live` and
  `tracking_column` for that API.
- In `table` mode, take over the stages that follow the operator through the
  stage contract (`ir::QueryStage`, `Describer::query_plan`, and
  `pushdown::render_plan`), following `from_duckdb`. Absorbed stages leave the
  pipeline and run with the backend's native semantics, which may differ from
  TQL for nulls, collations, `NaN`, and type coercion; document the
  differences on the operator's reference page. Accept the longest prefix that
  the backend spells, offer `pushdown=false` through `add_pushdown_argument`,
  and leave a user-provided `sql` query untouched.
- Compare text by the bytes that TQL sees. Default collations often ignore
  case and accents, and some comparisons pad the shorter operand: SQL Server
  treats `N'a' = N'a '` and `0x61 = 0x6100` as true. See the MySQL and SQL
  Server dialects for byte-exact spellings, and test them against rows that
  differ only in case, accents, trailing spaces, control characters, and NUL
  bytes.

## Destination operators

Database destination operators should use this common write surface:

- `table`
- `mode`
- `primary`

### Rules

- `table` names the destination relation.
- `mode` and `primary` should follow the `to_clickhouse` contract.
- Destination operators use the shared write surface of `table`, `mode`, and
  `primary`.

## Review checklist

When reviewing a database operator, check that it:

- uses the canonical argument names above
- keeps mutual exclusivity rules intact
- documents backend-specific extensions or unsupported shared arguments
  explicitly
- stays aligned with existing operators where semantics already exist
- documents an `## Optimizations` section on its reference page when it acts
  on optimizer hints (see `operators.md`)
