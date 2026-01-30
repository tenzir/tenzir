---
title: Fix intermittent UTF-8 errors in JSON parser
type: bugfix
authors:
  - jachris
  - claude
pr: 5698
created: 2026-01-29T15:32:43.155359Z
---

The JSON parser no longer intermittently fails with "The input is not valid UTF-8"
when parsing data containing multi-byte UTF-8 characters such as accented letters
or emojis.
