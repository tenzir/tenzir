Packages can now define reusable constants that any pipeline can reference, and the to_clickhouse operator supports native ClickHouse JSON columns. Nodes can also embed the platform control endpoint directly in their token for simpler onboarding.

## 🚀 Features

### Package constants

A package can now define constant `let` bindings in a `constants.tql` file at the package root. Each binding is evaluated to a constant when the package loads and can be referenced as `pkg::$name` from the package's own operators and pipelines, as well as from any external pipeline that uses the package.

```tql
// acme/constants.tql
let $high_severity = 8
let $threshold = $high_severity + 1
```

```tql
// any pipeline, once the acme package is available
where severity >= acme::$threshold
```

Later bindings may reference earlier ones, as `$threshold` does above.

*By @jachris in #6363.*

### Platform control endpoints in tokens

Tenzir nodes can now connect to the Tenzir Platform with a `tenzir.token` value that carries the platform control endpoint:

```yaml
tenzir:
  token: tnz_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX_<encoded-endpoint>
```

The same format works through the `TENZIR_TOKEN` environment variable. Set `tenzir.platform-control-endpoint` or `TENZIR_PLATFORM_CONTROL_ENDPOINT` only when you want to override the endpoint embedded in the token.

*By @tobim and @codex.*

### Support for ClickHouse JSON columns in to_clickhouse

The `to_clickhouse` operator can now send data to existing `JSON` and `Nullable(JSON)` columns on the ClickHouse server. Each event's field is serialized to JSON text and inserted, so records and their nested structures land in the column as JSON:

```tql
from {id: 0, payload: {a: 1, b: [2, 3]}}
to_clickhouse table="events", mode="append"
```

Absent values are written as `{}`. A null value becomes `{}` for a plain `JSON` column and SQL `NULL` for a `Nullable(JSON)` column. Because ClickHouse `JSON` columns only accept objects, non-record values are written as `{}` and a warning is emitted.

When creating a table, the new `json` argument selects top-level fields to create as `JSON` columns instead of the inferred type. It accepts a single field or a list of fields, and the columns are created even if the field is absent from the first event used for creation:

```tql
from {id: 0, payload: {a: 1, b: [2, 3]}}
to_clickhouse table="events", primary=id, json=[payload, raw]
```

*By @IyeOnline and @claude in #6381.*

## 🐞 Bug fixes

### from_file support for paths with spaces

The `from_file` operator now reads files whose path contains a space or other characters that require URI encoding. Previously, a space anywhere in the path (for example in a parent directory name) caused the pipeline to fail with a `failed to parse path as URI` error.

*By @IyeOnline and @claude in #6394.*
