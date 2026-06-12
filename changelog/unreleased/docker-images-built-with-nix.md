---
title: Docker images built with Nix
type: change
authors:
  - aljazerzen
  - claude
created: 2026-06-12T12:26:49.066237Z
---

The `tenzir`, `tenzir-node`, `tenzir-de`, `tenzir-node-de`, and `tenzir-demo`
Docker images are now built with Nix instead of a Dockerfile. The images keep
the same entrypoints, environment variables, volumes, working directory, and
non-root `tenzir` user (uid 999), so existing deployments and mounted volumes
continue to work unchanged.

The images no longer contain a Debian userland. Tools like `apt` are gone, and
shells must be invoked as `bash` (on `PATH`) rather than `/bin/sh`.

The `tenzir-deps` and `tenzir-dev` images are discontinued. They packaged the
Debian-based build environment, which no longer exists. Build Tenzir with Nix
instead by running `nix build` in the repository root.
