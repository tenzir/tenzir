---
title: "Check cURL response codes and do not deliver data on error"
type: bugfix
authors: IyeOnline
pr: 4660
---

We fixed a bug in the HTTP connectors, that caused them to not respect the
http response codes.
