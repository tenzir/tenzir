---
title: "Upgrade exporter to use new pipelines"
type: change
author: jachris
created: 2023-04-19T09:22:53Z
pr: 3076
---

The `exporter.*` metrics no longer exist, and will return in a future release as
a more generic instrumentation mechanism for all pipelines.
