---
title: "Fix possible desync in pending queries map"
type: bugfix
author: dominiklohmann
created: 2021-09-21T10:19:59Z
pr: 1884
---

The index now correctly cancels pending queries when the requester dies.
