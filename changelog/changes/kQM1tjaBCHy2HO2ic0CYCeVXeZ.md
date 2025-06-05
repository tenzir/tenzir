---
title: "Remove configure script"
type: change
authors: lava
pr: 1657
---

The `configure` script was removed. This was a custom script that
mimicked the functionality of an autotools-based `configure` script
by writing directly to the cmake cache. Instead, users now must use
the `cmake` and/or `ccmake` binaries directly to configure VAST.
