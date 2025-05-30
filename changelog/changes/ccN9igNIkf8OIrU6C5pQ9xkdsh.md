---
title: "Don't terminate `export` when used with `every`"
type: bugfix
authors: tobim
pr: 4382
---

Pipelines that use the `every` modifier with the `export` operator no longer
terminate after the first run.
