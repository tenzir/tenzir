---
title: "Downtone push behavior of pipelines"
type: bugfix
author: dominiklohmann
created: 2023-12-20T14:57:51Z
pr: 3758
---

We fixed a bug that caused operators that caused an increased memory usage for
pipelines with slow operators immediately after a faster operator.

We fixed a bug that caused short-running pipelines to sometimes hang.
