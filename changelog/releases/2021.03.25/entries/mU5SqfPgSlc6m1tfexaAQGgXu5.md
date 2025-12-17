---
title: "Report metrics while idle"
type: bugfix
author: dominiklohmann
created: 2021-03-17T12:30:19Z
pr: 1451
---

The archive, index, source, and sink components now report metrics when idle
instead of omitting them entirely. This allows for distinguishing between idle
and not running components from the metrics.
