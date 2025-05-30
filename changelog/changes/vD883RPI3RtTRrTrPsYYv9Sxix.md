---
title: "Enable `to_gogle_cloud_logging` for the Nix builds"
type: bugfix
authors: dominiklohmann
pr: 5154
---

The `to_google_cloud_logging` operator was not available in static binary builds
due to an oversight. This is now fixed.
