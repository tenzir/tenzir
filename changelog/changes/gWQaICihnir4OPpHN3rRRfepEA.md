---
title: "Support parsing numeric timestamps since epoch"
type: feature
authors: jachris
pr: 3927
---

When specifying a schema with a field typed as `time #unit=<unit>`, numeric
values will be interpreted as offsets from the epoch.
