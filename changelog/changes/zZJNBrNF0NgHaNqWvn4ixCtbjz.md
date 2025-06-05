---
title: "Move PCAP import/export into a plugin"
type: change
authors: dominiklohmann
pr: 1549
---

Plugins must define a separate entrypoint in their build scaffolding using the
argument `ENTRYPOINT` to the CMake function `VASTRegisterPlugin`. If only a
single value is given to the argument `SOURCES`, it is interpreted as the
`ENTRYPOINT` automatically.
