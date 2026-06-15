---
title: Compaction resolves package UDOs at startup
type: bugfix
author: raxyte
pr: 6210
created: 2026-05-20T17:40:00.000000Z
---

The `compaction` plugin no longer fails to start with
`module <package> not found` when a rule's `pipeline` references an
operator defined by an installed package. Previously, depending on the
order in which the node's components were initialized, the compactor's
eager rule-pipeline parse could run before the package manager had
published its operator modules to the global registry.
