---
title: "Fix log rotation threshold option"
type: bugfix
authors: dominiklohmann
pr: 1709
---

The `vast.log-rotation-threshold` option was silently ignored, causing VAST to
always use the default log rotation threshold of 10 MiB. The option works as
expected now.
