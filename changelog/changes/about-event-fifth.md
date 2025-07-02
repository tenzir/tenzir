---
title: "Fix crash in `from secret`"
type: change
authors: IyeOnline
pr: 5321
---

We fixed a crash in `from secret("key")`. It now gracefully rejects this, as
generic `from` cannot resolve secrets.
