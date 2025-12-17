---
title: "Move options from format to the import subcommand"
type: change
author: tobim
created: 2021-02-09T09:12:26Z
pr: 1354
---

The options `listen`, `read`, `schema`, `schema-file`, `type`, and `uds` can
from now on be supplied to the `import` command directly. Similarly, the options
`write` and `uds` can be supplied to the `export` command. All options can still
be used after the format subcommand, but that usage is deprecated.
