---
title: "Run commands from scripts"
type: feature
authors: rdettai
pr: 2446
---

The cloud execution commands (`run-lambda` and `execute-command`) now accept
scripts from file-like handles. To improve the usability of this feature, the
whole host file system is now mounted into the CLI container.
