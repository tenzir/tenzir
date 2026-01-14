---
title: "Prevent tcp socket inheritance to child processes"
type: bugfix
author: tobim
created: 2024-03-08T11:54:30Z
pr: 3998
---

We fixed a problem with the TCP connector that caused pipeline restarts on the
same port to fail if running `shell` or `python` operators were present.
