---
title: "Tailor expressions in filter operation"
type: bugfix
author: dominiklohmann
created: 2021-09-21T11:32:11Z
pr: 1885
---

Import filter expressions now work correctly with queries using field
extractors, e.g., `vast import suricata 'event_type == "alert"' <
path/to/eve.json`.
