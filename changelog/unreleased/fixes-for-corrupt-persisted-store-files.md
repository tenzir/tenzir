---
title: Fixes for corrupt persisted store files
type: bugfix
authors:
  - tobim
  - claude
prs:
  - 6471
created: 2026-07-24T10:29:01.085347Z
---

Tenzir nodes no longer risk writing stale memory or leftover file contents into
persisted store files.

Two defects that could corrupt data written to disk are fixed:

- Overwriting an existing file with shorter content no longer leaves trailing
  bytes from the previous version. This could previously produce store files
  with garbage after the payload when a temporary file path was reused.
- An internal buffer handle that had been moved from still appeared valid and
  could hand out memory it no longer kept alive, allowing freed and reused
  memory to end up in files written to disk. Moved-from handles now behave
  like empty ones.
