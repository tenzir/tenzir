---
title: "Fix two concurrency issues related to child process creation"
type: bugfix
author: tobim
created: 2024-06-26T15:38:19Z
pr: 4333
---

We fixed a bug that caused a "Bad file descriptor" error from the python
operator, when multiple instances of it were started simultaneously.
