---
title: "Improve `json` parser, add `null` type, and various fixes"
type: bugfix
authors: jachris
pr: 3503
---

The `json`, `suricata` and `zeek-json` parsers are now more stable and should
now parse all inputs correctly.

`null` records are no longer incorrectly transformed into records with `null`
fields anymore.
