---
title: "Fix HTTP PUT with empty request body"
type: bugfix
authors: mavam
pr: 4092
---

We fixed a bug in the `http` saver that prevented sending HTTP PUT requests with
an empty request body.
