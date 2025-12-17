---
title: "Dynamically grow simdjson buffer if necessary"
type: feature
author: IyeOnline
created: 2024-09-16T15:20:19Z
pr: 4590
---

The JSON parser is now able to also handle extremely large events when
not using the NDJSON or GELF mode.
