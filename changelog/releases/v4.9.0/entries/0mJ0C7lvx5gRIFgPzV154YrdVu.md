---
title: "Context versioning"
type: change
author: eliaskosunen
created: 2024-02-21T09:29:07Z
pr: 3945
---

The binary format used by contexts for saving on disk on node shutdown
is now versioned. A node can support loading of multiple different versions,
and automigrate between them.
