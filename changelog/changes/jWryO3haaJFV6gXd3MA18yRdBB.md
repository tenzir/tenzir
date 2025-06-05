---
title: "Eliminate shutdown lag from the signal monitor"
type: feature
authors: tobim
pr: 2766
---

The new `/query` endpoint for the experimental REST API allows users to receive
query data in multiple steps, as opposed to a oneshot export.
