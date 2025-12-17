---
title: "Handle quoted strings in CSV parser"
type: bugfix
author: dominiklohmann
created: 2021-06-09T08:07:50Z
pr: 1712
---

The `import csv` command handles quoted fields correctly. Previously, the
quotes were part of the parsed value, and field separators in quoted strings
caused the parser to fail.
