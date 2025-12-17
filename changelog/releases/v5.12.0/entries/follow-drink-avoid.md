---
title: "Optimization of the `delay` operator"
type: bugfix
author: jachris
created: 2025-08-02T15:25:38Z
pr: 5399
---

The `delay` operator optimization routine incorrectly declared that the behavior
of the operator does not depend on the order of its input. As a result, chains
such as `sort -> delay -> publish` did not correctly delay events.
