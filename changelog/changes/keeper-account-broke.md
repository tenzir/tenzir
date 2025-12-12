---
title: "Improved Google Cloud PubSub Integration"
type: feature
authors: IyeOnline
pr: 5593
---

We have improved our Google Cloud PubSub integration with the addition of the new
`from_google_cloud_pubsub` and `to_google_cloud_pubsub` operators.

These operators are direct *void -> event* and *event -> void* operators, which
means that they ensure a 1:1 relation between events and messages.

The `from_google_cloud_pubsub` operator can also attach metadata such
as message ID, publish time, and attributes for downstream enrichment.

The legacy `load_google_cloud_pubsub` and `save_google_cloud_pubsub` operators
are deprecated in favor of these event-preserving counterparts.
