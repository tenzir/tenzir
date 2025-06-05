---
title: "Add `--precise` mode to JSON parser"
type: feature
authors: jachris
pr: 4169
---

The `json` parser has a new `--precise` flag, which ensures that the layout of
the emitted events precisely match the input. For example, it guarantees that no
additional `null` fields will be added. This mode is implicitly enabled when
using `read gelf`.
