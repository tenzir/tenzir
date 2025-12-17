---
title: "Consolidate community and enterprise edition builds"
type: bugfix
author: tobim
created: 2024-04-26T13:13:19Z
pr: 4149
---

We fixed a misconfiguration that caused the `publish` and `subscribe` operators
not to be available in the statically linked Linux builds.

We fixed a crash on startup when selectively enabling or disabling plugins when
at least two plugins with dependent plugins were disabled.
