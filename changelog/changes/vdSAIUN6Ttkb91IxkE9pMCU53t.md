---
title: "Prefer reading query from stdin if available"
type: change
authors: dominiklohmann
pr: 1917
---

A recently added features allows for exporting everything when no query is
provided. We've restricted this to prefer reading a query from stdin if
available. Additionally, conflicting ways to read the query now trigger errors.
