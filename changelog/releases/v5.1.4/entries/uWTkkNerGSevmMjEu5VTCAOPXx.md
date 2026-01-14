---
title: "Fix lifetime issues and small bugs in `write_syslog`"
type: bugfix
author: raxyte
created: 2025-05-07T13:50:19Z
pr: 5180
---

We fixed a crash in `write_syslog` when receiving unexpected inputs and improved
some diagnostics.
