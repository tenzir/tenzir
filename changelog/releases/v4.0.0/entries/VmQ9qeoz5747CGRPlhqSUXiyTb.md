---
title: "Remove old commands"
type: change
author: dominiklohmann
created: 2023-05-25T11:49:16Z
pr: 3166
---

The `stop` command no longer exists. Shut down VAST nodes using CTRL-C instead.

The `version` command no longer exists. Use the more powerful `version` pipeline
operator instead.

The `spawn source` and `spawn sink` commands no longer exist. To import data
remotely, run a pipeline in the form of `remote from … | … | import`, and to
export data remotely, run a pipeline in the form of `export | … | remote to …`.

The lower-level `peer`, `kill`, and `send` commands no longer exist.
