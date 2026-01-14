---
title: "Check mmapped chunks for required minimum size"
type: bugfix
author: lava
created: 2024-12-12T11:53:03Z
pr: 4856
---

We fixed a bug introduced with v4.24.0 causing crashes on startup when some
of the files in the node's state directory were smaller than 12 bytes.
