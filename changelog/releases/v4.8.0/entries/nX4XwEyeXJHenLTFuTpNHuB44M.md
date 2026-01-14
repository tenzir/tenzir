---
title: "Display failing pipeline diagnostics in `/serve`"
type: bugfix
author: dominiklohmann
created: 2024-01-18T09:01:55Z
pr: 3788
---

The `/serve` API now displays why a pipeline became unavailable in an error case
instead of showing a generic error message. This causes runtime errors in
pipelines to show up in the Explorer on app.tenzir.com.
