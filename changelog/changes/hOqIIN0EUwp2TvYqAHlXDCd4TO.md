---
title: "Add the `ttl` to the `/pipeline/list` API"
type: feature
authors: dominiklohmann
pr: 4314
---

The `/pipeline/list` API now includes a new `ttl` field showing the TTL of the
pipeline. The remaining TTL moved from `ttl_expires_in_ns` to a `remaining_ttl`
field, aligning the output of the API with the `show pipelines` operator.
