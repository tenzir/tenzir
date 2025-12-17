---
title: "Increase the default segment size to 1 GiB"
type: change
author: mavam
created: 2020-11-13T20:34:05Z
pr: 1166
---

The default segment size in the archive is now 1 GiB. This reduces fragmentation
of the archive meta data and speeds up VAST startup time.
