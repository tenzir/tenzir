---
title: "Minimize the threadpool for client commands"
type: change
author: tobim
created: 2022-04-08T12:24:31Z
pr: 2193
---

Client commands such as `vast export` or `vast status` now create less threads
at runtime, reducing the risk of hitting system resource limits.
