---
title: "Improve handling of open file descriptors"
type: feature
authors: lava
pr: 3784
---

On Linux systems, the process metrics now have an additional
value `open_fds` showing the number of file descriptors
opened by the node.
