---
title: "Print timestamps with full precision for JSON"
type: change
author: dominiklohmann
created: 2020-06-09T20:24:18Z
pr: 909
---

The JSON export format now renders timestamps using strings instead of numbers
in order to avoid possible loss of precision.
