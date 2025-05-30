---
title: "Prefer CLI over config file for vast.plugins"
type: bugfix
authors: dominiklohmann
pr: 2289
---

The command-line options `--plugins`, `--plugin-dirs`, and `--schema-dirs` now
correctly overwrite their corresponding configuration options.
