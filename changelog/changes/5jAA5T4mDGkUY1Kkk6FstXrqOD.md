---
title: "Remove reader, writer, and language plugin types"
type: change
authors: dominiklohmann
pr: 3899
---

We removed the `tenzir-ctl start` subcommand. Users should switch to
the `tenzir-node` command instead, which accepts the same arguments
and presents the same command-line interface.
