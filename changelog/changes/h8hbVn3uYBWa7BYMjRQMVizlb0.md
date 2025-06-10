---
title: "Ignore static plugins when specified in config"
type: bugfix
authors: dominiklohmann
pr: 1528
---

VAST no longer erroneously tries to load explicitly specified plugins
dynamically that are linked statically.
