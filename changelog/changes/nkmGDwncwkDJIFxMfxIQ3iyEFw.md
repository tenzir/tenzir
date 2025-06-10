---
title: "Remove support for building without Arrow"
type: change
authors: dominiklohmann
pr: 1683
---

Apache Arrow is now a required dependency. The previously deprecated build
 option `-DVAST_ENABLE_ARROW=OFF` no longer exists.
