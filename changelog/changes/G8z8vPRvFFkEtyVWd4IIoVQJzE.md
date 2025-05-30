---
title: "Minimize the threadpool for client commands"
type: change
authors: tobim
pr: 2193
---

Client commands such as `vast export` or `vast status` now create less threads
at runtime, reducing the risk of hitting system resource limits.
