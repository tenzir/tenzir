---
title: "Make it easy to create docker images with Nix"
type: feature
author: tobim
created: 2022-12-17T17:14:02Z
pr: 2742
---

We now offer a `tenzir/vast-slim` image as an alternative to the `tenzir/vast`
image. The image is minimal in size and supports the same features as the
regular image, but does not support building additional plugins against it and
mounting in additional plugins.
