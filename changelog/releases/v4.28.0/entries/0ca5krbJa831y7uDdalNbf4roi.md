---
title: "Stop collecting metrics for hidden pipelines"
type: change
author: dominiklohmann
created: 2025-01-31T13:37:24Z
pr: 4966
---

`metrics "operator"` no longer includes metrics from hidden pipelines, such as
pipelines run under-the-hood by the Tenzir Platform.
