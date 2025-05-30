---
title: "Detect and report incomplete object in JSON parser"
type: bugfix
authors: jachris
pr: 3570
---

When using `read json`, incomplete objects (e.g., due to truncated files) are
now reported as an error instead of silently discarded.
