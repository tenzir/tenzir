---
title: "Fix index worker depletion"
type: bugfix
author: tobim
created: 2020-12-11T18:39:23Z
pr: 1225
---

The index no longer causes exporters to deadlock when the meta index produces
false positives.
