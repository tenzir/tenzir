---
title: "Fix operator parenthesis continuation"
type: bugfix
author: jachris
created: 2025-01-06T09:51:41Z
pr: 4885
---

Operator invocations that directly use parenthesis but continue after the
closing parenthesis are no longer rejected. For example, `where (x or y) and z`
is now being parsed correctly.
