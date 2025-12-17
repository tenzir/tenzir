---
title: "Precise Parsing"
type: bugfix
author: IyeOnline
created: 2024-09-27T11:20:57Z
pr: 4527
---

We fixed various edge cases in parsers where values would not be properly parsed
as typed data and were stored as plain text instead. No input data was lost, but
no valuable type information was gained either.
