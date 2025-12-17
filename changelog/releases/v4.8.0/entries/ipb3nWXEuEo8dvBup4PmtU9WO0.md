---
title: "Improve handling of open file descriptors"
type: feature
author: lava
created: 2024-01-15T09:58:40Z
pr: 3784
---

On Linux systems, the process metrics now have an additional
value `open_fds` showing the number of file descriptors
opened by the node.
