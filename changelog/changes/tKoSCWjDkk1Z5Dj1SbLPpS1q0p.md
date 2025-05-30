---
title: "Fix possible desync in pending queries map"
type: bugfix
authors: dominiklohmann
pr: 1884
---

The index now correctly cancels pending queries when the requester dies.
