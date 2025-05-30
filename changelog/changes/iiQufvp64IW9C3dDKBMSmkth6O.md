---
title: "Tailor expressions in filter operation"
type: bugfix
authors: dominiklohmann
pr: 1885
---

Import filter expressions now work correctly with queries using field
extractors, e.g., `vast import suricata 'event_type == "alert"' <
path/to/eve.json`.
