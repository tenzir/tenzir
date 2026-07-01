---
title: BITZ streams from neo pipelines
type: bugfix
authors:
  - tobim
  - codex
prs:
  - 6397
created: 2026-06-30T13:14:37.340644Z
---

Legacy `read_bitz` pipelines no longer report an unexpected internal error when they receive BITZ data produced by a neo-executed pipeline over TCP. This restores mixed-executor BITZ forwarding patterns such as `from "tcp://..." { read_bitz } | publish "..."`.
