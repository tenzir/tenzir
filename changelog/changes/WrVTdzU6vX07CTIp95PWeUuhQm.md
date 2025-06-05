---
title: "Add a --live option to the export operator"
type: feature
authors: tobim
pr: 3612
---

The `export` operator now has a `--live` option to continuously emit events as
they are imported instead of those that already reside in the database.
