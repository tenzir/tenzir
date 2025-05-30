---
title: "Implement the `geoip` context"
type: feature
authors: Dakostu
pr: 3731
---

The new `geoip` context is a built-in that reads MaxMind DB files and uses IP
values in events to enrich them with the MaxMind DB geolocation data.
