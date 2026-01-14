---
title: "Fix a hang in `if` for stalled inputs"
type: bugfix
author: dominiklohmann
created: 2025-05-14T13:13:45Z
pr: 5196
---

We fixed a regression in the `if` statement that caused it to indefinitely
withhold the last batch of events when its input stalled.
