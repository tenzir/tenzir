---
title: "Normalize GNUInstallDirs for external plugins"
type: bugfix
authors: dominiklohmann
pr: 1786
---

Plugins built against an external libvast no longer require the
`CMAKE_INSTALL_LIBDIR` to be specified as a path relative to the configured
`CMAKE_INSTALL_PREFIX`. This fixes an issue with plugins in separate packages
for some package managers, e.g., Nix.
