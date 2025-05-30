---
title: "Fix panic on parsing invalid syslog messages"
type: bugfix
authors: eliaskosunen
pr: 4012
---

Parsing an invalid syslog message (using the schema `syslog.unknown`)
no longer causes a crash.
