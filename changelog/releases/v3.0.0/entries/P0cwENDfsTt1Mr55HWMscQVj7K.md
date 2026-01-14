---
title: "Add 'pipeline' parameter and schematized format to export endpoint"
type: feature
author: lava
created: 2023-01-05T19:54:14Z
pr: 2773
---

The `/export` family of endpoints now accepts an optional `pipeline`
parameter to specify an ad-hoc pipeline that should be applied to
the exported data.
