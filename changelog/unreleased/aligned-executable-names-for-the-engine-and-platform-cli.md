---
title: Aligned executable names for the engine and platform CLI
type: change
authors:
  - mavam
created: 2026-10-13T08:30:22.920871Z
---

The executables behind the `tenzir` command now carry consistent names: `tenzir` remains the dispatcher, `tenzir-engine` is the engine with the classic pipeline CLI, and `tenzir-platform` is the platform CLI. Both helpers live in the `libexec` directory of the installation, outside of `PATH`. Running `libexec/tenzir-engine` directly now behaves like the classic `tenzir` command, for example `libexec/tenzir-engine 'from {x: 1}'`.
