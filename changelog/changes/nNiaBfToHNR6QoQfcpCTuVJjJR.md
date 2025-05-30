---
title: "Display store load failures to the user"
type: bugfix
authors: dominiklohmann
pr: 2507
---

Partitions now fail early when their stores fail to load from disk, detailing
what went wrong in an error message.
