---
title: "Evict old caches when exceeding capacity limits"
type: bugfix
authors: dominiklohmann
pr: 4984
---

We fixed an up to 60 seconds hang in requests to the `/serve` endpoint when the
request was issued after the pipeline with the corresponding `serve` operator
was started and before it finished with an error and without results.
