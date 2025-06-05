---
title: "Stop accepting new queries after initiating shutdown"
type: bugfix
authors: dominiklohmann
pr: 2215
---

VAST servers no longer accept queries after initiating shutdown. This fixes a
potential infinite hang if new queries were coming in faster than VAST was able
to process them.
