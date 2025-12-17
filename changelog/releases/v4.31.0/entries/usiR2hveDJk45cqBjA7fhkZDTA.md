---
title: "Hardcode the fluent-bit page size to 24576"
type: bugfix
author: tobim
created: 2025-03-27T08:43:08Z
pr: 5084
---

The `from_fluent_bit` and `to_fluent_bit` operators no longer crash when trying
to handle very large payloads.
