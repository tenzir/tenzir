---
title: "Add support for macOS-style syslog messages"
type: change
author: eliaskosunen
created: 2023-12-18T17:23:03Z
pr: 3692
---

The events created by the RFC 3164 syslog parser no longer has a `tag` field,
but `app_name` and `process_id`.
