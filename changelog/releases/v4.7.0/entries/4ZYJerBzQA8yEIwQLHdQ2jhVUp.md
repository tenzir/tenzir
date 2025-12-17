---
title: "Implement the `geoip` context"
type: feature
author: Dakostu
created: 2023-12-19T16:19:56Z
pr: 3731
---

The new `geoip` context is a built-in that reads MaxMind DB files and uses IP
values in events to enrich them with the MaxMind DB geolocation data.
