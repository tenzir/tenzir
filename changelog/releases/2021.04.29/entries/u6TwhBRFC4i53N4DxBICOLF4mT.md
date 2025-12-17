---
title: "Fix out-of-bounds access in command-line parser"
type: bugfix
author: lava
created: 2021-04-08T13:03:00Z
pr: 1536
---

The command-line parser no longer crashes when encountering a flag with missing
value in the last position of a command invocation.
