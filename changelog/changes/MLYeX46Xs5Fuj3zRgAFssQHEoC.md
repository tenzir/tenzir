---
title: "Remove events output from many context operators"
type: bugfix
authors: dominiklohmann
pr: 4143
---

The `enrich` operator sometimes stopped working when it encountered an event for
which the specified fields did not exist. This no longer happens.
