---
title: "Introduce a potpourri of smaller improvements"
type: bugfix
authors: dominiklohmann
pr: 2832
---

VAST now shuts down instantly when metrics are enabled instead of being held
alive for up to the duration of the telemetry interval (10 seconds).
