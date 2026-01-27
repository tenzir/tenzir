---
title: "`http` operator stalling"
type: bugfix
author: raxyte
created: 2025-09-18T13:25:51Z
pr: 5479
---

The `http` operator now correctly handles its internal waiting state, fixing an
intermittent issue where HTTP requests could hang unexpectedly.
