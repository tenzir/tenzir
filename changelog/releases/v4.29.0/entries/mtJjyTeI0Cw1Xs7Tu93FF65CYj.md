---
title: "Fix compilation error handling inside `if`"
type: bugfix
author: jachris
created: 2025-02-23T15:57:13Z
pr: 5011
---

A compilation error within an `if` statement no longer causes pipelines to
crash.
