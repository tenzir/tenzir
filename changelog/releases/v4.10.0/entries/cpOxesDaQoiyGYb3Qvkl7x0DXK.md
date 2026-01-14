---
title: "Fix panic on parsing invalid syslog messages"
type: bugfix
author: eliaskosunen
created: 2024-03-11T15:23:27Z
pr: 4012
---

Parsing an invalid syslog message (using the schema `syslog.unknown`)
no longer causes a crash.
