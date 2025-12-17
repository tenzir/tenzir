---
title: "Don't terminate `export` when used with `every`"
type: bugfix
author: tobim
created: 2024-07-12T09:53:07Z
pr: 4382
---

Pipelines that use the `every` modifier with the `export` operator no longer
terminate after the first run.
