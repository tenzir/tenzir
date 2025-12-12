---
title: "Removed `gcps://` URI scheme"
type: change
authors: IyeOnline
pr: 5593
---

We have removed the `gcps:/` URI scheme, which previously would dispatch to
`load_google_cloud_pubsub` and `save_google_cloud_pubsub`. As these operators
are deprecated and will be removed, the schemas are being retired as well.
