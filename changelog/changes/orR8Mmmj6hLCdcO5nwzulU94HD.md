---
title: "Add a nix package expression for VAST"
type: change
authors: tobim
pr: 740
---

The build system will from now on try use the CAF library from the system, if
one is provided. If it is not found, the CAF submodule will be used as a
fallback.
