---
title: "Hardcode the fluent-bit page size to 24576"
type: bugfix
authors: tobim
pr: 5084
---

The `from_fluent_bit` and `to_fluent_bit` operators no longer crash when trying
to handle very large payloads.
