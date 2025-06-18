---
title: "Return instantly in `/serve` if pipeline fails early"
type: bugfix
authors: dominiklohmann
pr: 4688
---

The `/serve` endpoint now returns instantly when its pipeline fails before the
endpoint is used for the first time. In the Tenzir Platform this causes the load
more button in the Explorer to correctly stop showing for pipelines that fail
shortly after starting.
