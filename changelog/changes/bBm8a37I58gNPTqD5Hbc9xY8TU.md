---
title: "Infer non-config types in extend and replace operators"
type: bugfix
authors: dominiklohmann
pr: 2768
---

The `replace` and `extend` pipeline operators wrongly inferred IP address,
subnet, pattern, and map values as strings. They are now inferred correctly. To
force a value to be inferred as a string, wrap it inside double quotes.
