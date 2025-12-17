---
title: "Move PCAP import/export into a plugin"
type: change
author: dominiklohmann
created: 2021-04-16T11:36:07Z
pr: 1549
---

Plugins must define a separate entrypoint in their build scaffolding using the
argument `ENTRYPOINT` to the CMake function `VASTRegisterPlugin`. If only a
single value is given to the argument `SOURCES`, it is interpreted as the
`ENTRYPOINT` automatically.
