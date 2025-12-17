---
title: "Fix debian package installation when a vast state directory exists"
type: bugfix
author: tobim
created: 2023-12-04T14:39:17Z
pr: 3705
---

The Debian package sometimes failed to install, and the bundled systemd unit
failed to start with Tenzir v4.6.2. This issue no longer exists.
