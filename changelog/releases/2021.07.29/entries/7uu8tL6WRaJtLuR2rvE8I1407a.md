---
title: "Support import filter expressions"
type: feature
author: dominiklohmann
created: 2021-07-09T12:41:34Z
pr: 1742
---

VAST now supports import filter expressions. They act as the dual to export
query expressions: `vast import suricata '#type == "suricata.alert"' < eve.json`
will import only `suricata.alert` events, discarding all other events.
