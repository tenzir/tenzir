---
title: "Add a nix package expression for VAST"
type: change
author: tobim
created: 2020-02-05T14:38:47Z
pr: 740
---

The build system will from now on try use the CAF library from the system, if
one is provided. If it is not found, the CAF submodule will be used as a
fallback.
