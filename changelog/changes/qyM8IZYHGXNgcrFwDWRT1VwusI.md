---
title: "Properly recover corrupted partition data on startup"
type: bugfix
authors: lava
pr: 2631
---

VAST now properly regenerates any corrupted, oversized partitions it encounters
during startup, provided that the corresponding store files are available. These
files could be produced by versions up to and including VAST v2.2, when using
configurations with an increased maximum partition size.
