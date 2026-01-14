---
title: "Fix shutdown of sources and importer"
type: bugfix
author: dominiklohmann
created: 2023-06-08T14:47:33Z
pr: 3207
---

Import processes sometimes failed to shut down automatically when the node
exited. They now shut down reliably.
