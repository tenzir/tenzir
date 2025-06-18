---
title: "Fix building VAST within a shallow git tree"
type: bugfix
authors: dominiklohmann
pr: 1570
---

VAST now correctly builds within shallow clones of the repository. If the build
system is unable to determine the correct version from `git-describe`, it now
always falls back to the version of the last release.
