---
title: "Split out `name` option and use metadata server when unset"
type: change
author: raxyte
created: 2025-04-30T11:29:49Z
pr: 5160
---

We split the `name` option of `to_google_cloud_logging`, no longer requiring
user to construct the ID manually.
