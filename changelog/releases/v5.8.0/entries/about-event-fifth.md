---
title: "Fix crash in `from secret`"
type: bugfix
author: IyeOnline
created: 2025-07-02T22:20:51Z
pr: 5321
---

We fixed a crash in `from secret("key")`. This is now gracefully rejected, as
generic `from` cannot resolve secrets.
