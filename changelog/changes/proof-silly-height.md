---
title: "`http` operator stalling"
type: bugfix
authors: raxyte
pr: 5479
---

The `http` operator now correctly handles its internal waiting state, fixing an
intermittent issue where HTTP requests could hang unexpectedly.
