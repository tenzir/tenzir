---
title: "Check cURL response codes and do not deliver data on error"
type: bugfix
author: IyeOnline
created: 2024-10-16T07:36:24Z
pr: 4660
---

We fixed a bug in the HTTP connectors, that caused them to not respect the
http response codes.
