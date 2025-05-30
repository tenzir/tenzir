---
title: "Add a timeout option to the export command"
type: feature
authors: dominiklohmann
pr: 1611
---

The new option `vast export --timeout=<duration>` allows for setting a timeout
for VAST queries. Cancelled exports result in a non-zero exit code.
