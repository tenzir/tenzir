---
title: "Use a unique version for plugins"
type: feature
author: dominiklohmann
created: 2021-07-09T16:42:49Z
pr: 1764
---

Plugin versions are now unique to facilitate debugging. They consist of three
optional parts: (1) the CMake project version of the plugin, (2) the Git
revision of the last commit that touched the plugin, and (3) a `dirty` suffix
for uncommited changes to the plugin. Plugin developers no longer need to
specify the version manually in the plugin entrypoint.
