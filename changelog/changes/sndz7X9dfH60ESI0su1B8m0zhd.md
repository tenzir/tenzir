---
title: "Make vast.conf lookup on Linux systems more intuitive"
type: feature
authors: dominiklohmann
pr: 1036
---

VAST now supports the XDG base directory specification: The `vast.conf` is now
found at `${XDG_CONFIG_HOME:-${HOME}/.config}/vast/vast.conf`, and schema files
at `${XDG_DATA_HOME:-${HOME}/.local/share}/vast/schema/`. The user-specific
configuration file takes precedence over the global configuration file in
`<sysconfdir>/vast/vast.conf`.
