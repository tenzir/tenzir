---
title: Shell source stdin closure
type: bugfix
authors:
  - mavam
  - chatgpt
pr: 6093
created: 2026-04-29T12:50:57.652339Z
---

The `shell` operator no longer hangs in source pipelines when the command waits for stdin to close. Commands such as `shell "cat >/dev/null; printf 'done\n'"` now reliably observe EOF and continue producing output.
