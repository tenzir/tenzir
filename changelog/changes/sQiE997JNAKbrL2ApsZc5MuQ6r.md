---
title: "Don't collect status from sources and sinks"
type: bugfix
authors: tobim
pr: 1234
---

The `vast status` command does not collect status information from sources and
sinks any longer. They were often too busy to respond, leading to a long delay
before the command completed.
