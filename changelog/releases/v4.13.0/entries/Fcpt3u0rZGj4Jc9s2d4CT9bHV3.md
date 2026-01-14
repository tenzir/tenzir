---
title: "Add LEEF parser"
type: bugfix
author: mavam
created: 2024-05-08T08:02:15Z
pr: 4178
---

The `syslog` parser no longer crops messages at unprintable characters, such as
tab (`\t`).

The `syslog` parser no longer eagerly attempts to grab an application name from
the content, fixing issues when combined with CEF and LEEF.
