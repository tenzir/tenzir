---
title: "Flush implicitly in the `import` operator"
type: feature
authors: dominiklohmann
pr: 3638
---

The `import` operator now flushes events to disk automatically before returning,
ensuring that they are available immediately for subsequent uses of the `export`
operator.
