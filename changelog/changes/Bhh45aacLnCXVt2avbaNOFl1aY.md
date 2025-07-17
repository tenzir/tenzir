---
title: "Add timeout to multiline syslog"
type: bugfix
authors: IyeOnline
pr: 4829
---

We fixed an oversight in the syslog parsers, which caused it to not yield an
event until the next line came in.
