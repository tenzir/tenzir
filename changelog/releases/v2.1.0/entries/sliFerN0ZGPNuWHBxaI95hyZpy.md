---
title: "Support environment variables for plugin options"
type: bugfix
author: dominiklohmann
created: 2022-06-30T11:34:50Z
pr: 2390
---

VAST no longer ignores environment variables for plugin-specific options. E.g.,
the environment variable `VAST_PLUGINS__FOO__BAR` now correctly refers to the
`bar` option of the `foo` plugin, i.e., `plugins.foo.bar`.
