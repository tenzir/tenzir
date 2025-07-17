---
title: "Restructure configuration file hierarchy"
type: change
authors: dominiklohmann
pr: 1073
---

All configuration options are now grouped into `vast` and `caf` sections,
depending on whether they affect VAST itself or are handed through to the
underlying actor framework CAF directly. Take a look at the bundled
`vast.yaml.example` file for an explanation of the new layout.
