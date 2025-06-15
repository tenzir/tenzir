---
title: "Fixed invalid JSON for small numbers"
type: bugfix
authors: jachris
pr: 5282
---

Operators such as `write_json` previously emitted invalid JSON for small
numbers. This also affected the Tenzir Platform as it invalidated some
responses, which could lead to no data showing up in the Explorer.
