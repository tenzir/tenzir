---
title: "Print timestamps with full precision for JSON"
type: change
authors: dominiklohmann
pr: 909
---

The JSON export format now renders timestamps using strings instead of numbers
in order to avoid possible loss of precision.
