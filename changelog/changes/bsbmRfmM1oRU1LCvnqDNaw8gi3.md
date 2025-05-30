---
title: "Always convert JSON null to VAST nil"
type: bugfix
authors: dominiklohmann
pr: 1009
---

Importing JSON no longer fails for JSON fields containing `null` when the
corresponding VAST type in the schema is a non-trivial type like
`vector<string>`.
