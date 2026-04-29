Tenzir can now consume from and publish to NATS JetStream subjects with from_nats and to_nats. This release also fixes crashes in static musl builds when evaluating deeply nested generated TQL expressions.

## 🚀 Features

### NATS JetStream operators

Tenzir can now consume from and publish to NATS JetStream subjects with `from_nats` and `to_nats`.

Use `from_nats` to receive one event per message. The raw payload appears in the `message` blob field, and `metadata_field` attaches NATS metadata:

```tql
from_nats "alerts", metadata_field=nats
parsed = string(message).parse_json()
```

Use `to_nats` to publish one message per event. By default, the operator serializes the whole event with `this.print_ndjson()`:

```tql
from {severity: "high", alert_type: "suspicious-login"}
to_nats "alerts"
```

Both operators support configurable connection settings, authentication, and the standard Tenzir `tls` record.

*By @mavam and @codex.*

## 🐞 Bug fixes

### Static musl builds no longer crash on deep TQL expressions

Static musl builds of `tenzir` no longer crash on deeply nested generated TQL expressions.

This affected generated pipelines with deeply nested expressions, for example rules or transformations that expand into long left-associated operator chains.

The `tenzir` binary now links with a larger default thread stack size on musl, which brings its behavior in line with non-static builds for these pipelines.

*By @tobim and @codex in #6082.*
