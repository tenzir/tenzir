---
title: "Align plugin and library output names"
type: change
authors: dominiklohmann
pr: 1527
---

Plugins configured via `vast.plugins` in the configuration file can now be
specified using either the plugin name or the full path to the shared plugin
library. We no longer allow omitting the extension from specified plugin files,
and recommend using the plugin name as a more portable solution, e.g., `example`
over `libexample` and `/path/to/libexample.so` over `/path/to/libexample`.
