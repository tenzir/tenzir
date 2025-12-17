---
title: "Fix log rotation threshold option"
type: bugfix
author: dominiklohmann
created: 2021-06-07T08:52:20Z
pr: 1709
---

The `vast.log-rotation-threshold` option was silently ignored, causing VAST to
always use the default log rotation threshold of 10 MiB. The option works as
expected now.
