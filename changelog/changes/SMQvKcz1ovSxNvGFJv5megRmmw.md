---
title: "Use proper full install dirs for system config"
type: bugfix
authors: dominiklohmann
pr: 1580
---

Specifying relative `CMAKE_INSTALL_*DIR` in the build configuration no longer
causes VAST not to pick up system-wide installed configuration files, schemas,
and plugins. The configured install prefix is now used correctly. The defunct
`VAST_SYSCONFDIR`, `VAST_DATADIR`, and `VAST_LIBDIR` CMake options no longer
exist. Use a combination of `CMAKE_INSTALL_PREFIX` and `CMAKE_INSTALL_*DIR`
instead.
