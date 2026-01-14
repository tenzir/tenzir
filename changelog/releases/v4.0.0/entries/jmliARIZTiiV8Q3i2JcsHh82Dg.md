---
title: "Implement the `serve` operator and `/serve` endpoint"
type: change
author: dominiklohmann
created: 2023-06-01T21:48:56Z
pr: 3180
---

The default port of the web plugin changed from 42001 to 5160. This change
avoids collisions from dynamic port allocation on Linux systems.
