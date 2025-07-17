---
title: "Handle Arrow decoder errors gracefully"
type: bugfix
authors: dominiklohmann
pr: 1247
---

Invalid Arrow table slices read from disk no longer trigger a segmentation
fault. Instead, the invalid on-disk state is ignored.
