---
title: "Fix hang for operators with infinite idle timeout"
type: bugfix
authors: dominiklohmann
pr: 5219
---

We fixed a hang in the `cache` and `buffer` operators when their input finished.
This also prevented the node from shutting down cleanly.
