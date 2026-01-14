---
title: "Remove events output from many context operators"
type: bugfix
author: dominiklohmann
created: 2024-04-23T07:14:13Z
pr: 4143
---

The `enrich` operator sometimes stopped working when it encountered an event for
which the specified fields did not exist. This no longer happens.
