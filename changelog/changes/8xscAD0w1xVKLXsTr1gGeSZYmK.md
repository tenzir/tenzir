---
title: "Respect `--connection-timeout` in more places"
type: bugfix
authors: dominiklohmann
pr: 2503
---

Configuration options representing durations with an associated command-line
option like `vast.connection-timeout` and `--connection-timeout` were not picked
up from configuration files or environment variables. This now works as
expected.
