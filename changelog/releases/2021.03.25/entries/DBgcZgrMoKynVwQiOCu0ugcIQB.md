---
title: "Fix possibly unhandled exception in disk monitor"
type: bugfix
author: dominiklohmann
created: 2021-03-17T13:26:10Z
pr: 1458
---

VAST no longer crashes when the disk monitor tries to calculate the size of the
database while files are being deleted. Instead, it will retry after the
configured scan interval.
