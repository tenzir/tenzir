---
title: "Fix blob parsing with padding"
type: bugfix
authors: jachris
pr: 3765
---

When reading Base64-encoded JSON strings with the `blob` type, `=` padding is
now accepted.
