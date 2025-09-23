---
title: "Hang in `every` and `cron`"
type: bugfix
authors: raxyte
pr: 5483
---

We fixed a bug in `every` and `cron` operators that could cause them to hang
and panic with assertions failures.
