---
title: "Fix possibly unhandled exception in disk monitor"
type: bugfix
authors: dominiklohmann
pr: 1458
---

VAST no longer crashes when the disk monitor tries to calculate the size of the
database while files are being deleted. Instead, it will retry after the
configured scan interval.
