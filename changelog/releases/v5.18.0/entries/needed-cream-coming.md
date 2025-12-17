---
title: "Fixed record sorting"
type: bugfix
author: mavam
created: 2025-10-17T14:35:58Z
pr: 5526
---

Calling `sort` on records may have caused a crash for more involved objects. This no longer happens.
