---
title: "Split out `name` option and use metadata server when unset"
type: change
authors: raxyte
pr: 5160
---

We split the `name` option of `to_google_cloud_logging`, no longer requiring
user to construct the ID manually.
