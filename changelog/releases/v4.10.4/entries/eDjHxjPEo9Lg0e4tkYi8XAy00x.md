---
title: "Fix a crash in the Sigma operator"
type: bugfix
author: dominiklohmann
created: 2024-03-13T13:12:21Z
pr: 4034
---

The `sigma` operator crashed for some rules when trying to attach the rule to
the matched event. This no longer happens.
