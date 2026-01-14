---
title: "Display store load failures to the user"
type: bugfix
author: dominiklohmann
created: 2022-08-19T12:46:53Z
pr: 2507
---

Partitions now fail early when their stores fail to load from disk, detailing
what went wrong in an error message.
