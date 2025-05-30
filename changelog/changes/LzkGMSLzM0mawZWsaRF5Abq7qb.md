---
title: "Run start commands asynchronously"
type: bugfix
authors: lava
pr: 2868
---

The start commands specified with the `vast.start.commands` option are now run
aynchronously. This means that commands that block indefinitely will no longer
prevent execution of subsequent commands, and allow for correct signal handling.
