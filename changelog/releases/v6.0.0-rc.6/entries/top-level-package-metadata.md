---
title: Top-level package metadata
type: bugfix
authors:
  - tobim
  - codex
pr: 6149
created: 2026-05-08T15:43:35.152192Z
---

Packages can now include a top-level `metadata` field for data consumed by external tools. Unknown package keys still fail validation, and the error now points users to `metadata` for non-engine package data.
