---
title: "Fix user shutdown handling for continuous exports"
type: feature
authors: dominiklohmann
pr: 779
---

Continuous export processes can now be stopped correctly. Before this change,
the node showed an error message and the exporting process exited with a
non-zero exit code.
