---
title: "Fix config parsing bug"
type: bugfix
author: dominiklohmann
created: 2025-05-20T12:35:43Z
pr: 5210
---

We fixed a bug that caused the 101st entry in objects by alphabetical order in
`tenzir.yaml` configuration files to be ignored.
