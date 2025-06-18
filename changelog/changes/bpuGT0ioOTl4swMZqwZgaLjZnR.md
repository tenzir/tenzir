---
title: "Add step size to disk monitor"
type: feature
authors: lava
pr: 1655
---

The new setting `vast.disk-monitor-step-size` enables the disk monitor
to remove *N* partitions at once before re-checking if the new size of the
database directory is now small enough. This is useful when checking the size
of a directory is an expensive operation itself, e.g., on compressed
filesystems.
