---
title: "Remove the transformer actor"
type: bugfix
author: Dakostu
created: 2023-02-02T13:45:39Z
pr: 2896
---

Pipelines that reduce the number of events do not prevent `vast export`
processes that have a `max-events` limit from terminating any more.
