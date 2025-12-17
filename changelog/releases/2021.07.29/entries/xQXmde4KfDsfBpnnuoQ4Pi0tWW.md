---
title: "Remove /etc as hardcoded sysconfdir from Nix build"
type: feature
author: dominiklohmann
created: 2021-07-16T10:18:27Z
pr: 1777
---

Installing VAST now includes a `vast.yaml.example` configuration file listing
all available options.
