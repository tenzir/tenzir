---
title: "Fix out-of-bounds access in command-line parser"
type: bugfix
authors: lava
pr: 1536
---

The command-line parser no longer crashes when encountering a flag with missing
value in the last position of a command invocation.
