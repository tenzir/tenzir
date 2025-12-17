---
title: "Handle assert gracefully in `write_syslog`"
type: bugfix
author: raxyte
created: 2025-05-12T17:04:53Z
pr: 5191
---

We now gracefully handle a panic in `write_syslog`, when `structured_data` does
not have the expected shape.
