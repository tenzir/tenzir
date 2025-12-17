---
title: "Support optional numeric duration output for JSON"
type: feature
author: dominiklohmann
created: 2021-05-05T10:12:20Z
pr: 1628
---

To enable easier post-processing, the new option
`vast.export.json.numeric-durations` switches JSON output of `duration` types
from human-readable strings (e.g., `"4.2m"`) to numeric (e.g., `252.15`) in
fractional seconds.
