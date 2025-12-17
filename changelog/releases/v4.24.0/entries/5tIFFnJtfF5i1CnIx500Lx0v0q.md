---
title: "Implement `append`, `prepend`, and `concatenate`"
type: feature
author: dominiklohmann
created: 2024-11-21T06:19:27Z
pr: 4792
---

The new `append`, `prepend`, and `concatenate` functions add an element to the
end of a list, to the front of a list, and merge two lists, respectively.
`xs.append(y)` is equivalent to `[...xs, y]`, `xs.prepend(y)` is equivalent to
`[y, ...xs]`, and `concatenate(xs, ys)` is equivalent to `[...xs, ..ys]`.
