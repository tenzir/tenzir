---
title: "Deprecate `vast.pipeline-triggers`"
type: change
author: dominiklohmann
created: 2023-03-10T13:49:26Z
pr: 3008
---

The `vast.pipeline-triggers` option is deprecated; while it continues to
work as-is, support for it will be removed in the next release. Use the
new inline import and export pipelines instead. They will return as more
generally applicable node ingress and egress pipelines in the future.
