---
title: "PRs 1343-1356-ngrodzitski"
type: feature
author: ngrodzitski
created: 2021-02-08T13:47:27Z
pr: 1343
---

The JSON import now always relies upon [simdjson](https://simdjson.org). The
previously experimental `--simdjson` option to the `vast import
json|suricata|zeek-json` commands no longer exist as the feature is considered
stable.
