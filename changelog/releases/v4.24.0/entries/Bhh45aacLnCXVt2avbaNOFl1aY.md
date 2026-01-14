---
title: "Add timeout to multiline syslog"
type: bugfix
author: IyeOnline
created: 2024-12-03T10:57:59Z
pr: 4829
---

We fixed an oversight in the syslog parsers, which caused it to not yield an
event until the next line came in.
