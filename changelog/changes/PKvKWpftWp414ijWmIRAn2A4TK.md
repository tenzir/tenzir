---
title: "Respect `--color` option in default implicit events sink"
type: bugfix
authors: dominiklohmann
pr: 5007
---

The implicit events sink of the `tenzir` binary now respects the
`--color=[always|never|auto]` option and the `NO_COLOR` environment variable.
Previously, color usage was only determined based on whether `stdout` had a TTY
attached.
