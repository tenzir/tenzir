---
title: Homebrew puts tenzir on PATH
type: feature
authors:
  - mavam
created: 2026-11-15T11:08:36.255848Z
---

Installing Tenzir with `brew install --cask tenzir/tenzir/tenzir` now links the `tenzir` command into Homebrew's `bin` directory, so it is on your `PATH` without adding `/opt/tenzir/bin` by hand. Uninstalling the cask removes the link.
