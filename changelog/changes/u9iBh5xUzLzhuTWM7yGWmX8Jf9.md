---
title: "Increase the default segment size to 1 GiB"
type: change
authors: mavam
pr: 1166
---

The default segment size in the archive is now 1 GiB. This reduces fragmentation
of the archive meta data and speeds up VAST startup time.
