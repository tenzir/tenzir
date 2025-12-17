---
title: "Ignore static plugins when specified in config"
type: bugfix
author: dominiklohmann
created: 2021-04-08T10:02:55Z
pr: 1528
---

VAST no longer erroneously tries to load explicitly specified plugins
dynamically that are linked statically.
