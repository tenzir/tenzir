---
title: "Fix configured pipelines causing a crash when they contain a syntax error"
type: bugfix
authors: Dakostu
pr: 4334
---

Shutting down a node no longer sets managed pipelines to the completed state
unintentionally.

Configured pipelines with retry on error enabled will not trigger an assertion
anymore when they fail to launch.
