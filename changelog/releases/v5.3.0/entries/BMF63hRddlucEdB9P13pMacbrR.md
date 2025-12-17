---
title: "Fix overflow warning for `-9223372036854775808`"
type: bugfix
author: jachris
created: 2025-05-24T16:39:19Z
pr: 5223
---

The lowest 64-bit integer, `-9223372036854775808`, no longer causes an overflow
warning.
