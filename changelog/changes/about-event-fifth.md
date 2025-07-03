---
title: "Fix crash in `from secret`"
type: bugfix
authors: IyeOnline
pr: 5321
---

We fixed a crash in `from secret("key")`. This is now gracefully rejected, as
generic `from` cannot resolve secrets.
