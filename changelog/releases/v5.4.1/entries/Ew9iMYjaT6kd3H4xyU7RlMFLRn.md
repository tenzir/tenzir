---
title: "Fixed invalid JSON for small numbers"
type: bugfix
author: jachris
created: 2025-06-13T20:05:36Z
pr: 5282
---

Operators such as `write_json` previously emitted invalid JSON for small
numbers. This also affected the Tenzir Platform as it invalidated some
responses, which could lead to no data showing up in the Explorer.
