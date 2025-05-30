---
title: "Improve metrics collection"
type: change
authors: dominiklohmann
pr: 3982
---

Nodes now collect CPU, disk, memory, and process metrics every second instead of
every ten seconds, improving the usability of metrics with the `chart` operator.
Memory metrics now work as expected on macOS.
