---
title: "Fix config parsing bug"
type: bugfix
authors: dominiklohmann
pr: 5210
---

We fixed a bug that caused the 101st entry in objects by alphabetical order in
`tenzir.yaml` configuration files to be ignored.
