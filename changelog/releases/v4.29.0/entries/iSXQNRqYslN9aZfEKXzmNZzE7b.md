---
title: "Fix a crash in `{parse,read}_grok` for invalid patterns"
type: bugfix
author: dominiklohmann
created: 2025-02-25T15:34:01Z
pr: 5018
---

The `read_grok` operator and `parse_grok` functions no longer crash when
providing an invalid Grok expression.
