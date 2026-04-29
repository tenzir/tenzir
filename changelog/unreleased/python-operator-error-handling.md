---
title: Python operator error handling
type: bugfix
authors:
  - mavam
  - codex
pr: 6095
created: 2026-04-29T13:11:09.960899Z
---

The `python` operator no longer hangs when user code fails. Pipelines now terminate promptly and report Python syntax or runtime errors instead of timing out.
