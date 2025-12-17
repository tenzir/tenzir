---
title: "PRs 1223-1328-1334-1390-a4z"
type: change
author: dominiklohmann
created: 2020-12-10T13:44:00Z
pr: 1223
---

VAST switched to [spdlog >= 1.5.0](https://github.com/gabime/spdlog) for
logging. For users, this means: The `vast.console-format` and `vast.file-format`
now must be specified using the spdlog pattern syntax as described
[here](https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#pattern-flags).
All settings under `caf.logger.*` are now ignored by VAST, and only the `vast.*`
counterparts are used for logger configuration.
