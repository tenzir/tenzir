---
title: "Remove deprecated `as_secs()` function"
type: change
author: dominiklohmann
created: 2025-05-14T11:30:41Z
pr: 5190
---

We removed the deprecated `<duration>.as_secs()` function which has been
superseded by `<duration>.count_seconds()` quite some time ago. The
`count_seconds()` function provides the same functionality with a more
consistent naming convention that aligns with other duration-related functions
like `count_minutes()`, `count_hours()`, etc.
