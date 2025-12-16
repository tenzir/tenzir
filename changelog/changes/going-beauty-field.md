---
title: "Support GOOGLE_CLOUD_PROJECT environment variable in `to_google_cloud_logging` operator"
type: feature
authors: lava
pr: 5591
---

The `to_google_cloud_logging` operator now checks for the `GOOGLE_CLOUD_PROJECT` environment variable
if no explicit project id is given, before falling back to the Google Metadata service.
