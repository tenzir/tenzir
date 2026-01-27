---
title: "Add multi-line syslog message support"
type: change
author: eliaskosunen
created: 2024-04-08T13:26:16Z
pr: 4080
---

Lines of input containing an invalid syslog messages are now assumed to
be a continuation of a message on a previous line, if there's any.
