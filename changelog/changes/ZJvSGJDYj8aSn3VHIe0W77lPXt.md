---
title: "Remove the transformer actor"
type: bugfix
authors: Dakostu
pr: 2896
---

Pipelines that reduce the number of events do not prevent `vast export`
processes that have a `max-events` limit from terminating any more.
