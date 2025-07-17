---
title: "CPU limits in containers"
type: bugfix
authors: dominiklohmann
pr: 5288
---

Nodes now correctly respect cgroup CPU limits on Linux. Previously, such limits were ignored, and the node always used the physical number of cores available, unless a lower number was explicitly configured through the `caf.scheduler.max-threads` option. This bug fix may improve performance and resource utilization for nodes running in environments with such limitations.
