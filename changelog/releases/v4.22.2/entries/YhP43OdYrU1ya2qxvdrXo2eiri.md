---
title: "Allow reinstantiating the `buffer` operator"
type: bugfix
author: dominiklohmann
created: 2024-10-25T16:44:42Z
pr: 4702
---

We fixed a bug in the `buffer` operator that caused it to break when
restarting a pipeline or using multiple buffers in a "parallel" context,
such as in `load_tcp`'s pipeline argument.
