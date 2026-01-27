---
title: "Invalid scientific notation when using `write_json`"
type: bugfix
author: jachris
created: 2025-06-12T13:20:56Z
pr: 5274
---

When using `write_json` with large floating-point numbers, the resulting JSON
was ill-formed. For example, the number `5483819555176798000.0` was previously
printed as `5.483819555176798e+18.0`. The extra `.0` at the end is not valid
JSON. Thus, the output was rejected by some parsers. Now, `write_json` renders
this number as `5.483819555176798e+18` instead.

This bug was also observable on the Tenzir Platform, where it could lead to
request timeouts. Now, large numbers are shown correctly.
