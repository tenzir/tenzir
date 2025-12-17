---
title: "Implement the `serve` operator and `/serve` endpoint"
type: feature
author: dominiklohmann
created: 2023-06-01T21:48:56Z
pr: 3180
---

The `serve` operator and `/serve` endpoint supersede the experimental `/query`
endpoint. The operator is a sink for events, and bridges a pipeline into a
RESTful interface from which events can be pulled incrementally.
