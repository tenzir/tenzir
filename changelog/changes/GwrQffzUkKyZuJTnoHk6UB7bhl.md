---
title: "Dynamically grow simdjson buffer if necessary"
type: feature
authors: IyeOnline
pr: 4590
---

The JSON parser is now able to also handle extremely large events when
not using the NDJSON or GELF mode.
