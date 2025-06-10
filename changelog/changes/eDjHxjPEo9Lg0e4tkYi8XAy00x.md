---
title: "Fix a crash in the Sigma operator"
type: bugfix
authors: dominiklohmann
pr: 4034
---

The `sigma` operator crashed for some rules when trying to attach the rule to
the matched event. This no longer happens.
