---
title: "Correctly handle duplicate serve requests and tune some logs"
type: bugfix
author: tobim
created: 2024-11-01T09:54:20Z
pr: 4715
---

We eliminated a rare crash in the `serve` operator that was introduced in
v4.20.3.
