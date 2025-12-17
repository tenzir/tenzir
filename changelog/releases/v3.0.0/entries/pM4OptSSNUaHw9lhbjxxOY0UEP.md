---
title: "PRs 2877-2904-2907"
type: feature
author: Dakostu
created: 2023-01-27T10:55:24Z
pr: 2877
---

The `export` and `import` commands now support an optional pipeline string
that allows for chaining pipeline operators together and executing such a
pipeline on outgoing and incoming data. This feature is experimental and the
syntax is subject to change without notice. New operators are only available in
the new pipeline syntax, and the old YAML syntax is deprecated.
