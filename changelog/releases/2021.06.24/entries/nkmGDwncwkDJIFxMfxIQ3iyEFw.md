---
title: "Remove support for building without Arrow"
type: change
author: dominiklohmann
created: 2021-05-27T13:41:13Z
pr: 1683
---

Apache Arrow is now a required dependency. The previously deprecated build
 option `-DVAST_ENABLE_ARROW=OFF` no longer exists.
