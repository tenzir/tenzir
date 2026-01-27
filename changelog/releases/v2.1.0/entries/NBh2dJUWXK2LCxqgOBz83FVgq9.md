---
title: "Prefer CLI over config file for vast.plugins"
type: bugfix
author: dominiklohmann
created: 2022-05-19T18:45:10Z
pr: 2289
---

The command-line options `--plugins`, `--plugin-dirs`, and `--schema-dirs` now
correctly overwrite their corresponding configuration options.
