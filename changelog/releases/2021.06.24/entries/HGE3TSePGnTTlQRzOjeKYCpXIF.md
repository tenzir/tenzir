---
title: "Add support for per-plugin configuration files"
type: feature
author: dominiklohmann
created: 2021-06-17T11:19:14Z
pr: 1724
---

Plugins load their respective configuration from
`<configdir>/vast/plugin/<plugin-name>.yaml` in addition to the regular
configuration file at `<configdir>/vast/vast.yaml`. The new plugin-specific file
does not require putting configuration under the key `plugins.<plugin-name>`.
This allows for deploying plugins without needing to touch the
`<configdir>/vast/vast.yaml` configuration file.
