---
title: "Implement the `serve` operator and `/serve` endpoint"
type: change
authors: dominiklohmann
pr: 3180
---

The default port of the web plugin changed from 42001 to 5160. This change
avoids collisions from dynamic port allocation on Linux systems.
