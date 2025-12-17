---
title: "Add the `buffer` operator for breaking back pressure"
type: bugfix
author: dominiklohmann
created: 2024-07-19T11:40:45Z
pr: 4404
---

Metrics emitted towards the end of an operator's runtime were sometimes not
recorded correctly. This now works reliably.
