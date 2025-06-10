---
title: "Handle assert gracefully in `write_syslog`"
type: bugfix
authors: raxyte
pr: 5191
---

We now gracefully handle a panic in `write_syslog`, when `structured_data` does
not have the expected shape.
