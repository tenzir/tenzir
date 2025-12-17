---
title: "Fail late in the python operator setup"
type: bugfix
author: tobim
created: 2024-03-22T12:30:21Z
pr: 4066
---

The `python` operator often failed with a 504 Gateway Timeout error on
app.tenzir.com when first run. This no longer happens.
