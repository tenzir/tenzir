---
title: "Install example configuration files to datarootdir"
type: change
authors: dominiklohmann
pr: 1880
---

Example configuration files are now installed to the datarootdir as opposed to
the sysconfdir in order to avoid overriding previously installed configuration
files.
