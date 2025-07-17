---
title: "Load plugin schemas after built-in schemas"
type: feature
authors: dominiklohmann
pr: 1608
---

Plugin schemas are now installed to `<datadir>/vast/plugin/<plugin>/schema`,
while VAST's built-in schemas reside in `<datadir>/vast/schema`. The load order
guarantees that plugins are able to reliably override the schemas bundled with
VAST.
