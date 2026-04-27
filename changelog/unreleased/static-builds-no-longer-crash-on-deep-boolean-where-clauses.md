---
title: Static builds no longer crash on deep boolean where clauses
type: bugfix
authors:
  - tobim
  - codex
pr: 6082
created: 2026-04-27T13:26:45.949058Z
---

Static `tenzir` builds no longer crash when evaluating very long boolean `where` clauses, such as generated detection rules with large `or` chains.

This affected large generated filters with dozens of boolean terms, for example detection rules that expand into a single `where` clause with many repeated `or` conditions.

These pipelines now behave consistently across static and non-static builds.
