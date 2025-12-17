---
title: "Update response format of the /export endpoint"
type: change
author: lava
created: 2023-02-01T14:59:14Z
pr: 2899
---

For the experimental REST API, the result format of the `/export` endpoint
was modified: The `num_events` key was renamed to `num-events`, and the
`version` key was removed.
