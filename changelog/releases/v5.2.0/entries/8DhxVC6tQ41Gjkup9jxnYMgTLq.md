---
title: "Handle empty files in `read_xsv` & friends"
type: bugfix
author: IyeOnline
created: 2025-05-22T16:06:23Z
pr: 5215
---

We fixed a bug that caused `read_xsv` & friends to crash when trying to read an
empty file.
