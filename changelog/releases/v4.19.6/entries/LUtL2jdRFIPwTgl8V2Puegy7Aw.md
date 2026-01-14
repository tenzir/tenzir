---
title: "Implement the `assert` operator"
type: feature
author: dominiklohmann
created: 2024-08-14T15:36:25Z
pr: 4506
---

The `tenzir` command-line utility gained a new option `--strict`, causing it to
exit with a non-zero exit code for pipelines that emit at least one warning.
