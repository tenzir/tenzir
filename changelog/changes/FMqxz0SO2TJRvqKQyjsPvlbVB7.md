---
title: "Add an operator blocklist"
type: feature
authors: dominiklohmann
pr: 3642
---

The `tenzir.disable-plugins` option is a list of names of plugins and builtins
to explicitly forbid from being used in Tenzir. For example, adding `shell`
will prohibit use of the `shell` operator builtin, and adding `kafka` will
prohibit use of the `kafka` connector plugin. This allows for a more
fine-grained control than the `tenzir.allow-unsafe-pipelines` option.
