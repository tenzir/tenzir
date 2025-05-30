---
title: "Fix healthcheck in docker-compose.yaml"
type: feature
authors: lo-chr
pr: 5126
---

`context update <name>` for `lookup-table` contexts now supports per-entry
timeouts. The `--create-timeout <duration>` option sets the time after which
lookup table entries expire, and the `--update-timeout <duration>` option sets
the time after which lookup table entries expire if they are not accessed.
