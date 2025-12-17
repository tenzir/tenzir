---
title: "Fix printing of non-null intrusive pointers"
type: bugfix
author: lava
created: 2021-03-10T13:23:24Z
pr: 1430
---

Some non-null pointers were incorrectly rendered as `*nullptr` in log messages.
