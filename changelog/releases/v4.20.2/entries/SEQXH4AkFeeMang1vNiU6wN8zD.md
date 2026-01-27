---
title: "Fix the `azure-log-analytics` operator (again)"
type: bugfix
author: dominiklohmann
created: 2024-09-06T12:04:01Z
pr: 4578
---

The `azure-log-analytics` operator sometimes errored on startup complaining
about am unknown `window` option. This no longer occurs.
