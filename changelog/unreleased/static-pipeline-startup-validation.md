---
title: Static pipeline startup validation
type: bugfix
authors:
  - tobim
  - codex
prs:
  - 6248
created: 2026-06-02T10:53:53.017453Z
---

Tenzir now validates all configured and packaged pipelines before starting any of them during node startup. Previously, a deployment with multiple configured pipelines could start some valid pipelines and only then abort after discovering that a later pipeline was invalid.
