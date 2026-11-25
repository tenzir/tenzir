---
title: Site-provided tools for Nix builds
type: feature
authors:
  - tobim
created: 2026-11-24T14:33:52.370358Z
---

Nix builders can mount their build tools at `/run/tenzir-build-tools/bin` to use them when building Tenzir. Set `TENZIR_BUILD_TOOLS` before entering a development shell to select a different directory.
