---
title: Platform connection during node startup
type: bugfix
authors:
  - tobim
  - codex
prs:
  - 6268
created: 2026-06-09T12:51:34.44394Z
---

Tenzir Nodes now connect to the Tenzir Platform during startup before waiting
for the local index to become available. Previously, Platform connectivity
could be delayed while the index was still initializing.
