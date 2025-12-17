---
title: "Handle Arrow decoder errors gracefully"
type: bugfix
author: dominiklohmann
created: 2020-12-18T18:41:07Z
pr: 1247
---

Invalid Arrow table slices read from disk no longer trigger a segmentation
fault. Instead, the invalid on-disk state is ignored.
