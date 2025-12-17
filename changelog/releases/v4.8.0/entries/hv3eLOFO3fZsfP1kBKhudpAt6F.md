---
title: "Fix blob parsing with padding"
type: bugfix
author: jachris
created: 2024-01-11T11:14:06Z
pr: 3765
---

When reading Base64-encoded JSON strings with the `blob` type, `=` padding is
now accepted.
