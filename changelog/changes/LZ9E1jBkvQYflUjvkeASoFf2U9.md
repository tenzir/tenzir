---
title: "Fix start command detection for spdlog"
type: bugfix
authors: dominiklohmann
pr: 1530
---

Custom commands from plugins ending in `start` no longer try to write to the
server instead of the client log file.
