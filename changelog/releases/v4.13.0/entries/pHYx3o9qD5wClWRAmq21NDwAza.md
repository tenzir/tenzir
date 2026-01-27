---
title: "Add `--precise` mode to JSON parser"
type: feature
author: jachris
created: 2024-05-06T14:26:34Z
pr: 4169
---

The `json` parser has a new `--precise` flag, which ensures that the layout of
the emitted events precisely match the input. For example, it guarantees that no
additional `null` fields will be added. This mode is implicitly enabled when
using `read gelf`.
