---
title: "Fix `fork` operator stopping after initial events"
type: bugfix
author: raxyte
created: 2025-09-01T09:44:56Z
pr: 5450
---

We fixed a bug where the `fork` operator would stop processing events after handling
only the first few events, causing data loss in downstream pipeline stages.
