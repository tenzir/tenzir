---
title: "PRs 1230-1246-1281-1314-1315-ngrodzitski"
type: feature
author: dominiklohmann
created: 2021-01-13T16:27:09Z
pr: 1230
---

VAST relies on [simdjson](https://github.com/simdjson/simdjson) for JSON
parsing. The substantial gains in throughput shift the bottleneck of the ingest
path from parsing input to indexing at the node. To use the (yet experimental)
feature, use `vast import json|suricata|zeek-json --simdjson`.
