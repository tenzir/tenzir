---
title: "Merge contents of all configuration files"
type: feature
authors: dominiklohmann
pr: 1040
---

VAST now merges the contents of all used configuration files instead of using
only the most user-specific file. The file specified using `--config` takes the
highest precedence, followed by the user-specific path
`${XDG_CONFIG_HOME:-${HOME}/.config}/vast/vast.conf`, and the compile-time path
`<sysconfdir>/vast/vast.conf`.
