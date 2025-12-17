---
title: "Publishing to dynamic topics"
type: feature
author: dominiklohmann
created: 2025-06-24T09:34:55Z
pr: 5294
---

The `publish` operator now allows for dynamic topics to be derived from each
individual event.

For example, assuming Suricata logs, `publish f"suricata.{event_type}"` now
publishes to the topic `suricata.alert` for alert events and `suricata.flow` for
flow events. This works with any expression that evaluates to a string,
including `publish @name` to use the schema name of the event.
