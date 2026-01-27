---
title: Phantom pipeline entries with empty IDs
type: bugfix
authors:
  - jachris
  - claude
pr: 5680
created: 2026-01-22T22:09:49.81626Z
---

In rare cases, a phantom pipeline with an empty ID could appear in the pipeline list that couldn't be deleted through the API.
