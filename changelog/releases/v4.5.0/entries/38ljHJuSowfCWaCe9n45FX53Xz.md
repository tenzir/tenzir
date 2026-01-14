---
title: "Ensure exporter metrics don't get lost"
type: bugfix
author: tobim
created: 2023-11-13T17:40:31Z
pr: 3633
---

The `exporter.*` metrics will now be emitted in case the exporter finishes
early.
