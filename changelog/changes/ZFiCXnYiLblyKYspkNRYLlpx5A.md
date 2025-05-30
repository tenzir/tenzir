---
title: "Fix overzealous parameter validation in `/pipeline/launch`"
type: change
authors: dominiklohmann
pr: 4919
---

Contexts persist less frequently now in the background, reducing their resource
usage.
