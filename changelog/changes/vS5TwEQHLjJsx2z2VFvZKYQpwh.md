---
title: "Fix crash in `sigma` operator for non-existent file"
type: bugfix
authors: dominiklohmann
pr: 4010
---

The `sigma` operator sometimes crashed when pointed to a non-existent file or
directory. This no longer happens.
