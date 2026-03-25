---
title: JSON parse error context
type: change
authors:
  - IyeOnline
pr: 5805
created: 2026-03-06T00:00:00.000000Z
---

JSON parsing errors now display the surrounding bytes at the error location. This
makes it easier to diagnose malformed JSON in your data pipelines.

For example, if your JSON is missing a closing bracket, the error message shows
you the bytes around that location and marks where the parser stopped expecting
more input.
