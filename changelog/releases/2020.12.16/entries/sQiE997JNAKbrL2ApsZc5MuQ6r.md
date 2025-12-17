---
title: "Don't collect status from sources and sinks"
type: bugfix
author: tobim
created: 2020-12-15T20:44:57Z
pr: 1234
---

The `vast status` command does not collect status information from sources and
sinks any longer. They were often too busy to respond, leading to a long delay
before the command completed.
