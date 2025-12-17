---
title: "Fix build without Arrow"
type: bugfix
author: dominiklohmann
created: 2021-05-21T14:03:45Z
pr: 1673
---

VAST and transform plugins now build without Arrow support again.

The `delete` transform step correctly deletes fields from the layout when
running VAST with Arrow disabled.
