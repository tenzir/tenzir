---
title: "Remove reader, writer, and language plugin types"
type: change
author: dominiklohmann
created: 2024-02-01T11:56:10Z
pr: 3899
---

We removed the `tenzir-ctl start` subcommand. Users should switch to
the `tenzir-node` command instead, which accepts the same arguments
and presents the same command-line interface.
