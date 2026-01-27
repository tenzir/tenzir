---
title: "Add a --live option to the export operator"
type: feature
author: tobim
created: 2023-11-06T12:34:28Z
pr: 3612
---

The `export` operator now has a `--live` option to continuously emit events as
they are imported instead of those that already reside in the database.
