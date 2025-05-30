---
title: "Fix overflow warning for `-9223372036854775808`"
type: bugfix
authors: jachris
pr: 5223
---

The lowest 64-bit integer, `-9223372036854775808`, no longer causes an overflow
warning.
