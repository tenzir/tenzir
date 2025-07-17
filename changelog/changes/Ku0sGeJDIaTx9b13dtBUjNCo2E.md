---
title: "Add per-layout metrics to imports"
type: feature
authors: dominiklohmann
pr: 1781
---

VAST now exports per-layout import metrics under the key
`<reader>.events.<layout-name>` in addition to the regular `<reader>.events`.
This makes it easier to understand the event type distribution.
