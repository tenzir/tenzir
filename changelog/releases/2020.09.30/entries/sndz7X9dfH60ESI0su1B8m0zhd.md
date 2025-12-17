---
title: "Make vast.conf lookup on Linux systems more intuitive"
type: feature
author: dominiklohmann
created: 2020-09-02T14:41:59Z
pr: 1036
---

VAST now supports the XDG base directory specification: The `vast.conf` is now
found at `${XDG_CONFIG_HOME:-${HOME}/.config}/vast/vast.conf`, and schema files
at `${XDG_DATA_HOME:-${HOME}/.local/share}/vast/schema/`. The user-specific
configuration file takes precedence over the global configuration file in
`<sysconfdir>/vast/vast.conf`.
