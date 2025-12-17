---
title: "Fix overzealous parameter validation in `/pipeline/launch`"
type: change
author: dominiklohmann
created: 2025-01-21T14:19:38Z
pr: 4919
---

Contexts persist less frequently now in the background, reducing their resource
usage.
