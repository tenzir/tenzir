---
title: "Add percentage of total number of events to index status"
type: feature
authors: dominiklohmann
pr: 2351
---

The index statistics in `vast status --detailed` now show the event distribution
per schema as a percentage of the total number of events in addition to the
per-schema number, e.g., for `suricata.flow` events under the key
`index.statistics.layouts.suricata.flow.percentage`.
