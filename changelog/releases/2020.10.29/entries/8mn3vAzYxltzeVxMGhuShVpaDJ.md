---
title: "Fix merging of source status objects"
type: bugfix
author: dominiklohmann
created: 2020-10-22T11:10:11Z
pr: 1109
---

The `vast status --detailed` command now correctly shows the status of all
sources, i.e., `vast import` or `vast spawn source` commands.
