---
title: "Stop collecting metrics for hidden pipelines"
type: change
authors: dominiklohmann
pr: 4966
---

`metrics "operator"` no longer includes metrics from hidden pipelines, such as
pipelines run under-the-hood by the Tenzir Platform.
