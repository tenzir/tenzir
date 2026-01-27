---
title: "Do not refuse startup when `pid.lock` is invalid"
type: bugfix
author: dominiklohmann
created: 2025-04-30T13:47:49Z
pr: 5164
---

The node no longer refuses to start when its last shutdown happened in the brief
period on startup after its PID file was created and before it was flushed.
