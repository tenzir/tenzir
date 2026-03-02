---
title: Fix CEF parsing for unescaped equals
type: bugfix
author: jachris
pr: 5841
created: 2026-03-02T13:48:57.26831Z
---

The CEF parser now handles unescaped `=` characters (which are not conforming to
the specification) by using a heuristic.
