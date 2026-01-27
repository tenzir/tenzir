---
title: "Fix `python` operator"
type: bugfix
author: dominiklohmann
created: 2025-06-03T09:47:12Z
pr: 5258
---

Tenzir Node v5.3.0 contained a mismatched version of the Python operator,
causing the operator to fail to start. This no longer happens.
