---
title: "Report metrics while idle"
type: bugfix
authors: dominiklohmann
pr: 1451
---

The archive, index, source, and sink components now report metrics when idle
instead of omitting them entirely. This allows for distinguishing between idle
and not running components from the metrics.
