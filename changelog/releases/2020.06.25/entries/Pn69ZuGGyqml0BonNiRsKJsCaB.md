---
title: "Improve handling of UTF-8 input"
type: bugfix
author: dominiklohmann
created: 2020-06-09T17:14:17Z
pr: 910
---

The `export json` command now correctly unescapes its output.

VAST now correctly checks for control characters in inputs.
