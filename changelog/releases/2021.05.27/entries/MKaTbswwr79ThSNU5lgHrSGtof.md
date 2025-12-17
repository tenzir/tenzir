---
title: "Add a timeout option to the export command"
type: feature
author: dominiklohmann
created: 2021-05-03T11:57:14Z
pr: 1611
---

The new option `vast export --timeout=<duration>` allows for setting a timeout
for VAST queries. Cancelled exports result in a non-zero exit code.
