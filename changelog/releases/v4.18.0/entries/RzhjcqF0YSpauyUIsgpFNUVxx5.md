---
title: "Fix possible crash when one of multiple subscribers disconnects"
type: bugfix
author: dominiklohmann
created: 2024-07-03T12:59:10Z
pr: 4346
---

We fixed a rare crash when one of multiple `subscribe` operators for the same
topic disconnected while at least one of the other subscribers was overwhelmed
and asked for corresponding publishers to throttle.
