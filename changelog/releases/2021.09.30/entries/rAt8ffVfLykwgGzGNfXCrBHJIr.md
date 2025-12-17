---
title: "Install example configuration files to datarootdir"
type: change
author: dominiklohmann
created: 2021-09-14T07:03:34Z
pr: 1880
---

Example configuration files are now installed to the datarootdir as opposed to
the sysconfdir in order to avoid overriding previously installed configuration
files.
