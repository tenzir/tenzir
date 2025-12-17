---
title: "Fix logging in systemd when built without support"
type: bugfix
author: tobim
created: 2021-08-20T09:44:56Z
pr: 1857
---

VAST now only switches to journald style logging by default when it is actually
supported.
