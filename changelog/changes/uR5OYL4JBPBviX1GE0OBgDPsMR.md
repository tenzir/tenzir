---
title: "Prevent tcp socket inheritance to child processes"
type: bugfix
authors: tobim
pr: 3998
---

We fixed a problem with the TCP connector that caused pipeline restarts on the
same port to fail if running `shell` or `python` operators were present.
