---
title: "Fix building VAST within a shallow git tree"
type: bugfix
author: dominiklohmann
created: 2021-04-23T08:12:20Z
pr: 1570
---

VAST now correctly builds within shallow clones of the repository. If the build
system is unable to determine the correct version from `git-describe`, it now
always falls back to the version of the last release.
