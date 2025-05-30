---
title: "Align endpoints between regular and slim Docker images"
type: bugfix
authors: dominiklohmann
pr: 3137
---

The `tenzir/vast` image now listens on `0.0.0.0:5158` instead of
`127.0.0.1:5158` by default, which aligns the behavior with the
`tenzir/vast-slim` image.
