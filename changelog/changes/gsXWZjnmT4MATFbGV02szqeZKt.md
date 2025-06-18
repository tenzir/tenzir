---
title: "Fix build without Arrow"
type: bugfix
authors: dominiklohmann
pr: 1673
---

VAST and transform plugins now build without Arrow support again.

The `delete` transform step correctly deletes fields from the layout when
running VAST with Arrow disabled.
