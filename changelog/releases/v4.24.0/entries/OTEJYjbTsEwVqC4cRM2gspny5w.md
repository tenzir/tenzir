---
title: "Implement `where` and `map` on lists"
type: feature
author: dominiklohmann
created: 2024-11-20T02:00:34Z
pr: 4788
---

The `<list>.map(<capture>, <expression>)` function replaces each value from
`<list>` with the value from `<expression>`. Within `<expression>`, the elements
are available as `<capture>`. For example, to add 5 to all elements in the list
`xs`, use `xs = xs.map(x, x + 5)`.

The `<list>.where(<capture>, <predicate>)` removes all elements from `<list>`
for which the `<predicate>` evaluates to `false`. Within `<predicate>`, the
elements are available as `<capture>`. For example, to remove all elements
smaller than 3 from the list `xs`, use `xs = xs.where(x, x >= 3)`.
