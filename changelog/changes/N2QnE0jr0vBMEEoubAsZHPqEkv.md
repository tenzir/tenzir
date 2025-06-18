---
title: "Fix index worker depletion"
type: bugfix
authors: tobim
pr: 1225
---

The index no longer causes exporters to deadlock when the meta index produces
false positives.
