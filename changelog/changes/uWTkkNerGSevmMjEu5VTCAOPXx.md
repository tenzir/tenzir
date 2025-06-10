---
title: "Fix lifetime issues and small bugs in `write_syslog`"
type: bugfix
authors: raxyte
pr: 5180
---

We fixed a crash in `write_syslog` when receiving unexpected inputs and improved
some diagnostics.
