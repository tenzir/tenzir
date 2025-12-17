---
title: "Fix crash in `sigma` operator for non-existent file"
type: bugfix
author: dominiklohmann
created: 2024-03-11T12:42:55Z
pr: 4010
---

The `sigma` operator sometimes crashed when pointed to a non-existent file or
directory. This no longer happens.
