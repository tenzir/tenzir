---
title: "Restore implicit `read json` in `from tcp`"
type: bugfix
author: dominiklohmann
created: 2024-04-30T13:27:40Z
pr: 4175
---

We accidentally removed the implicit `read json` from `from tcp` in Tenzir
v4.12. The shortform now works as expected again.
