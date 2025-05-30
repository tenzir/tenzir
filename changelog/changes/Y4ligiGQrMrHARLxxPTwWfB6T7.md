---
title: "Remove -c short option for setting config file"
type: feature
authors: dominiklohmann
pr: 781
---

The short option `-c` for setting the configuration file has been removed. The
long option `--config` must now be used instead. This fixed a bug that did not
allow for `-c` to be used for continuous exports.
