This release fixes crashes in static musl builds when evaluating deeply nested generated TQL expressions.

## 🐞 Bug fixes

### Static musl builds no longer crash on deep TQL expressions

Static musl builds of `tenzir` no longer crash on deeply nested generated TQL expressions.

This affected generated pipelines with deeply nested expressions, for example rules or transformations that expand into long left-associated operator chains.

The `tenzir` binary now links with a larger default thread stack size on musl, which brings its behavior in line with non-static builds for these pipelines.

*By @tobim and @codex in #6082.*
