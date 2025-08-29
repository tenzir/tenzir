---
title: "Fix `fork` operator stopping after initial events"
type: bugfix
authors: raxyte
pr: 5450
---

We fixed a bug where the `fork` operator would stop processing events after handling
only the first few events, causing data loss in downstream pipeline stages.
