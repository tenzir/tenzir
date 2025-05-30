---
title: "Add LEEF parser"
type: bugfix
authors: mavam
pr: 4178
---

The `syslog` parser no longer crops messages at unprintable characters, such as
tab (`\t`).

The `syslog` parser no longer eagerly attempts to grab an application name from
the content, fixing issues when combined with CEF and LEEF.
