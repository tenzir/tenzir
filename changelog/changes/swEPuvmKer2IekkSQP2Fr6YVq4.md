---
title: "Restore implicit `read json` in `from tcp`"
type: bugfix
authors: dominiklohmann
pr: 4175
---

We accidentally removed the implicit `read json` from `from tcp` in Tenzir
v4.12. The shortform now works as expected again.
