---
title: "Downtone push behavior of pipelines"
type: bugfix
authors: dominiklohmann
pr: 3758
---

We fixed a bug that caused operators that caused an increased memory usage for
pipelines with slow operators immediately after a faster operator.

We fixed a bug that caused short-running pipelines to sometimes hang.
