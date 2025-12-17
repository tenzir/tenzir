---
title: "Remove deprecated format-specific options"
type: change
author: dominiklohmann
created: 2021-04-06T19:49:15Z
pr: 1529
---

The previously deprecated usage
([#1354](https://github.com/tenzir/vast/pull/1354)) of format-independent
options after the format in commands is now no longer possible. This affects the
options `listen`, `read`, `schema`, `schema-file`, `type`, and `uds` for import
commands and the `write` and `uds` options for export commands.
