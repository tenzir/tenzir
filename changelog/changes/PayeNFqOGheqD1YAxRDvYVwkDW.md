---
title: "Deduplicate plugin entrypoint in sources"
type: bugfix
authors: dominiklohmann
pr: 1573
---

We fixed a regression that made it impossible to build static binaries from
outside of the repository root directory.

The `VASTRegisterPlugin` CMake function now correctly removes the `ENTRYPOINT`
from the given `SOURCES`, allowing for plugin developers to easily glob for
sources again.
