---
title: "Newlines before `else`"
type: bugfix
author: jachris
created: 2025-07-17T10:31:59Z
pr: 5348
---

Previously, the `if … { … } else { … }` construct required that there was no
newline before `else`. This restriction is now lifted, which allows placing
`else` at the beginning of the line:

```tql
if x { … }
else if y { … }
else { … }
```
