---
title: "Make JSON field selectors configurable"
type: feature
author: dominiklohmann
created: 2021-11-25T10:55:47Z
pr: 1974
---

JSON field selectors are now configurable instead of being hard-coded for
Suricata Eve JSON and Zeek Streaming JSON. E.g., `vast import json
--selector=event_type:suricata` is now equivalent to `vast import suricata`.
This allows for easier integration of JSONL data containing a field that
indicates its type.
