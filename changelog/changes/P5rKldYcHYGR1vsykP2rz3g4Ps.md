---
title: "Correctly handle duplicate serve requests and tune some logs"
type: bugfix
authors: tobim
pr: 4715
---

We eliminated a rare crash in the `serve` operator that was introduced in
v4.20.3.
