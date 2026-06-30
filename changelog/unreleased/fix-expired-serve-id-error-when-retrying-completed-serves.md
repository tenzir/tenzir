---
title: Fix expired serve id error when retrying completed serves
type: bugfix
authors:
  - lava
prs:
  - 6373
created: 2026-06-24T23:31:08.017863Z
---

Retrying a `/serve` or `/serve-multi` request after its pipeline finished no longer fails with an "expired serve id" error. The endpoint now reports the stream as completed and, when you retry with the continuation token of the final poll, re-delivers that last batch of events. This makes polling robust against lost or cancelled responses, which previously could turn a finished stream into a hard error.
