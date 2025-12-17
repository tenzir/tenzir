---
title: "Fix configured pipelines causing a crash when they contain a syntax error"
type: bugfix
author: Dakostu
created: 2024-06-27T07:33:10Z
pr: 4334
---

Shutting down a node no longer sets managed pipelines to the completed state
unintentionally.

Configured pipelines with retry on error enabled will not trigger an assertion
anymore when they fail to launch.
