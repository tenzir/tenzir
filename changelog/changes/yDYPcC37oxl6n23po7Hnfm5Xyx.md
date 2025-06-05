---
title: "Fix two concurrency issues related to child process creation"
type: bugfix
authors: tobim
pr: 4333
---

We fixed a bug that caused a "Bad file descriptor" error from the python
operator, when multiple instances of it were started simultaneously.
