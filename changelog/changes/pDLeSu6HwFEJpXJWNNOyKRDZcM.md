---
title: "Fix `export --live` and introduce `metrics`"
type: bugfix
authors: dominiklohmann
pr: 3790
---

`export --live` sometimes got stuck, failing to deliver events. This no longer
happens.
