---
title: "Remove -c short option for setting config file"
type: feature
author: dominiklohmann
created: 2020-03-06T09:23:18Z
pr: 781
---

The short option `-c` for setting the configuration file has been removed. The
long option `--config` must now be used instead. This fixed a bug that did not
allow for `-c` to be used for continuous exports.
