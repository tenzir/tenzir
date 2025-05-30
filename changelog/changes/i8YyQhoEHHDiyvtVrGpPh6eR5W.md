---
title: "Add the `buffer` operator for breaking back pressure"
type: bugfix
authors: dominiklohmann
pr: 4404
---

Metrics emitted towards the end of an operator's runtime were sometimes not
recorded correctly. This now works reliably.
