---
title: "Improve handling of UTF-8 input"
type: bugfix
authors: dominiklohmann
pr: 910
---

The `export json` command now correctly unescapes its output.

VAST now correctly checks for control characters in inputs.
