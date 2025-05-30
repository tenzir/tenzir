---
title: "Display failing pipeline diagnostics in `/serve`"
type: bugfix
authors: dominiklohmann
pr: 3788
---

The `/serve` API now displays why a pipeline became unavailable in an error case
instead of showing a generic error message. This causes runtime errors in
pipelines to show up in the Explorer on app.tenzir.com.
