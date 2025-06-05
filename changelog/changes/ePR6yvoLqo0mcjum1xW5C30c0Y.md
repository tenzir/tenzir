---
title: "Fail late in the python operator setup"
type: bugfix
authors: tobim
pr: 4066
---

The `python` operator often failed with a 504 Gateway Timeout error on
app.tenzir.com when first run. This no longer happens.
