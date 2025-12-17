---
title: "Dedicated Syslog Schema Names"
type: change
author: IyeOnline
created: 2025-09-16T16:44:23Z
pr: 5472
---

The `read_syslog` operator now produces dedicated schemas `syslog.rfc5425`,
`syslog.rfc3164` and `syslog.unknown` instead of an unspecific `tenzir.syslog`.
