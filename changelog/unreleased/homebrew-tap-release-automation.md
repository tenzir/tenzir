---
title: Homebrew tap release automation for macOS packages
type: feature
authors:
  - Matthias Vallentin
pr: 5876
created: 2026-03-08T00:00:00.000000Z
---

Tenzir now prepares its macOS installer package for Homebrew cask distribution
by normalizing the package identifier to `com.tenzir.tenzir.runtime` and
updating the `tenzir/homebrew-tenzir` tap from release automation.
