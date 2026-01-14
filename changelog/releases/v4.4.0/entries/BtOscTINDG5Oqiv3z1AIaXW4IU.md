---
title: "Detect and report incomplete object in JSON parser"
type: bugfix
author: jachris
created: 2023-10-16T12:41:50Z
pr: 3570
---

When using `read json`, incomplete objects (e.g., due to truncated files) are
now reported as an error instead of silently discarded.
