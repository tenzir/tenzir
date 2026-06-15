---
title: Configured pipelines with package operators
type: bugfix
authors:
  - mavam
  - codex
created: 2026-05-04T13:54:37.228784Z
---

Configured startup pipelines can now reference operators from static packages reliably. Previously, such pipelines could fail during node startup with `module <package> not found`, even though the same package operator worked when run manually after startup.
