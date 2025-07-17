---
title: "Update the main repository to include timestamped pipelines"
type: feature
authors: Dakostu
pr: 3869
---

`show pipelines` and the `/pipeline` API endpoints now include `created_at` and `last_modified` fields that track the pipeline's creation and last manual modification time, respectively. Pipelines created with older versions of Tenzir will use the start time of the node as their creation time.
