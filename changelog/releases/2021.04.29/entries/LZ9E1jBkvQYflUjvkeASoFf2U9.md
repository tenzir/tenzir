---
title: "Fix start command detection for spdlog"
type: bugfix
author: dominiklohmann
created: 2021-04-07T08:12:01Z
pr: 1530
---

Custom commands from plugins ending in `start` no longer try to write to the
server instead of the client log file.
