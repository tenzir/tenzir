---
title: "Update response format of the /export endpoint"
type: change
authors: lava
pr: 2899
---

For the experimental REST API, the result format of the `/export` endpoint
was modified: The `num_events` key was renamed to `num-events`, and the
`version` key was removed.
