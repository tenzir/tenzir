---
title: "Precise Parsing"
type: change
author: IyeOnline
created: 2024-09-27T11:20:57Z
pr: 4527
---

The JSON parser's `--precise` option is now deprecated, as the "precise" mode
is the new default. Use `--merge` to get the previous "imprecise" behavior.

The JSON parser's `--no-infer` option has been renamed to `--schema-only`. The
old name is deprecated and will be removed in the future.
