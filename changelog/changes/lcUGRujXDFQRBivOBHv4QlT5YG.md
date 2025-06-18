---
title: "Add Bloom filter meta index"
type: feature
authors: mavam
pr: 931
---

The meta index now uses Bloom filters for equality queries involving IP
addresses. This especially accelerates queries where the user wants to know
whether a certain IP address exists in the entire database.
