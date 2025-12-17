---
title: "Hang in `every` and `cron`"
type: bugfix
author: raxyte
created: 2025-09-24T11:50:00Z
pr: 5483
---

We fixed a bug in `every` and `cron` operators that could cause them to hang
and panic with assertions failures.
