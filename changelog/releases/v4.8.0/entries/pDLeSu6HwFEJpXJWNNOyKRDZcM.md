---
title: "Fix `export --live` and introduce `metrics`"
type: bugfix
author: dominiklohmann
created: 2024-01-11T19:08:26Z
pr: 3790
---

`export --live` sometimes got stuck, failing to deliver events. This no longer
happens.
