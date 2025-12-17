---
title: "Improve `json` parser, add `null` type, and various fixes"
type: bugfix
author: jachris
created: 2023-09-21T11:24:25Z
pr: 3503
---

The `json`, `suricata` and `zeek-json` parsers are now more stable and should
now parse all inputs correctly.

`null` records are no longer incorrectly transformed into records with `null`
fields anymore.
