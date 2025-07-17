---
title: "Remove /etc as hardcoded sysconfdir from Nix build"
type: bugfix
authors: dominiklohmann
pr: 1777
---

The static binary no longer behaves differently than the regular build with
regards to its configuration directories: system-wide configuration files now
reside in `<prefix>/etc/vast/vast.yaml` rather than `/etc/vast/vast.yaml`.
