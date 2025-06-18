---
title: "Make TQL2 the default"
type: change
authors: dominiklohmann
pr: 5086
---

TQL2 is now the default and only option for writing pipelines with Tenzir. The
environment variable `TENZIR_TQL2`, the configuration option `tenzir.tql2` have
no more effect. Using the command-line option `--tql2` results in an error.
