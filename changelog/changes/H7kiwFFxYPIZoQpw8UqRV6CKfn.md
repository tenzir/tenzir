---
title: "Support hard-kill for unresponsive actors"
type: bugfix
authors: mavam
pr: 1005
---

The shutdown process of the server process could potentially hang forever. VAST
now uses a 2-step procedure that first attempts to terminate all components
cleanly. If that fails, it will attempt a hard kill afterwards, and if that
fails after another timeout, the process will call `abort(3)`.
