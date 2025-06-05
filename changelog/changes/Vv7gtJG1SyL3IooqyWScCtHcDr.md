---
title: "Add '--disk-budget-check-binary' option to disk monitor"
type: feature
authors: lava
pr: 1453
---

The disk monitor gained a new `vast.start.disk-budget-check-binary` option that
can be used to specify an external binary to determine the size of the database
directory. This can be useful in cases where `stat()` does not give the correct
answer, e.g. on compressed filesystems.
