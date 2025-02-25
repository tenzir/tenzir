The implicit events sink of the `tenzir` binary now respects the
`--color=[always|never|auto]` option and the `NO_COLOR` environment variable.
Previously, color usage was only determined based on whether `stdout` had a TTY
attached.
