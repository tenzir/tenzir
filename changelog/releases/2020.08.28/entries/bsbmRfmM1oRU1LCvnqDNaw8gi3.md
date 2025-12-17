---
title: "Always convert JSON null to VAST nil"
type: bugfix
author: dominiklohmann
created: 2020-08-07T10:54:09Z
pr: 1009
---

Importing JSON no longer fails for JSON fields containing `null` when the
corresponding VAST type in the schema is a non-trivial type like
`vector<string>`.
