This release fixes a scheduling issue introduced in v5.24.0 that could cause the node to become unresponsive when too many pipelines using detached operators were deployed simultaneously.

## 🐞 Bug fixes

### Scheduling issue with detached operators

Fixed a scheduling issue introduced in v5.24.0 that could cause the node to become unresponsive when too many pipelines using detached operators like `from_udp` were deployed simultaneously.

*By @lava in #5895.*
