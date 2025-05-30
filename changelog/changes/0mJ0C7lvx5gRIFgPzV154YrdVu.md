---
title: "Context versioning"
type: change
authors: eliaskosunen
pr: 3945
---

The binary format used by contexts for saving on disk on node shutdown
is now versioned. A node can support loading of multiple different versions,
and automigrate between them.
