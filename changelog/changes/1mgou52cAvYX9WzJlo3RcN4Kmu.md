---
title: "Fix logging in systemd when built without support"
type: bugfix
authors: tobim
pr: 1857
---

VAST now only switches to journald style logging by default when it is actually
supported.
