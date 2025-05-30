---
title: "Fix shutdown of sources and importer"
type: bugfix
authors: dominiklohmann
pr: 3207
---

Import processes sometimes failed to shut down automatically when the node
exited. They now shut down reliably.
