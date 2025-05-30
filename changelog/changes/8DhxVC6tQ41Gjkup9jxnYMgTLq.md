---
title: "Handle empty files in `read_xsv` & friends"
type: bugfix
authors: IyeOnline
pr: 5215
---

We fixed a bug that caused `read_xsv` & friends to crash when trying to read an
empty file.
