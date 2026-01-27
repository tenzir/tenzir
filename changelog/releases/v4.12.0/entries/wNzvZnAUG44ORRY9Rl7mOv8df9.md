---
title: "Fix HTTP PUT with empty request body"
type: bugfix
author: mavam
created: 2024-04-04T20:34:21Z
pr: 4092
---

We fixed a bug in the `http` saver that prevented sending HTTP PUT requests with
an empty request body.
