---
title: Fixed assertion failure when encountering duplicate keys
type: bugfix
authors:
  - IyeOnline
pr: 5612
created: 2025-12-19T13:28:32.61271Z
---

We fixed an assertion failure and subsequent crash that could occur when parsing
events that contain duplicate keys.
