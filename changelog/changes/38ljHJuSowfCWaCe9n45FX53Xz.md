---
title: "Ensure exporter metrics don't get lost"
type: bugfix
authors: tobim
pr: 3633
---

The `exporter.*` metrics will now be emitted in case the exporter finishes
early.
