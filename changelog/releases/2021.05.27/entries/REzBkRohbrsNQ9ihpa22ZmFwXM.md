---
title: "Fix install dirs wrt binary relocatability"
type: bugfix
author: dominiklohmann
created: 2021-05-07T13:47:45Z
pr: 1624
---

Non-relocatable VAST binaries no longer look for configuration, schemas, and
plugins in directories relative to the binary location. Vice versa, relocatable
VAST binaries no longer look for configuration, schemas, and plugins in their
original install directory, and instead always use paths relative to their
binary location. On macOS, we now always build relocatable binaries.
Relocatable binaries now work correctly on systems where the libary install
directory is `lib64` instead of `lib`.
