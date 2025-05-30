---
title: "Fix printing of non-null intrusive pointers"
type: bugfix
authors: lava
pr: 1430
---

Some non-null pointers were incorrectly rendered as `*nullptr` in log messages.
