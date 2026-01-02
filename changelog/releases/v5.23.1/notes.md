This release fixes internal errors in expression evaluation for heterogeneous data, resolves a crash in the operator when using , and ensures the connector shuts down gracefully.

## 🐞 Bug fixes

### Assertion failure in deduplicate with count_field

The `deduplicate` operator with `count_field` option could cause assertion failures when discarding events.

*By @raxyte.*

### Graceful shutdown for save_tcp connector

The `save_tcp` connector now gracefully shuts down on pipeline stop and connection failures. Previously, the connector could abort the entire application on exit.

*By @raxyte in #5637.*

### Length mismatch in expression evaluation for heterogeneous data

Expression evaluation could produce a length mismatch when processing heterogeneous data, potentially causing assertion failures. This affected various operations including binary and unary operators, field access, indexing, and aggregation functions.

*By @raxyte and @codex.*
