---
title: "Fix user shutdown handling for continuous exports"
type: feature
author: dominiklohmann
created: 2020-03-04T12:04:57Z
pr: 779
---

Continuous export processes can now be stopped correctly. Before this change,
the node showed an error message and the exporting process exited with a
non-zero exit code.
