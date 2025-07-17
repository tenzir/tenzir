---
title: "PRs 1143-1157-1160-1165"
type: change
authors: dominiklohmann
pr: 1143
---

The on-disk format for table slices now supports versioning of table slice
encodings. This breaking change makes it so that adding further encodings or
adding new versions of existing encodings is possible without breaking again in
the future.
