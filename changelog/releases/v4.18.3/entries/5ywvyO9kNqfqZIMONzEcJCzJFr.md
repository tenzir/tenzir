---
title: "Use `append_array_slice` everywhere"
type: bugfix
author: jachris
created: 2024-07-16T15:32:33Z
pr: 4394
---

Fixed an issue where `null` records were sometimes transformed into non-null
records with `null` fields.

We fixed an issue that sometimes caused `subscribe` to fail when multiple
`publish` operators pushed to the same topic at the exact same time.
